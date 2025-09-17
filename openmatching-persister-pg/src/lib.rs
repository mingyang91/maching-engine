use std::{collections::BTreeMap, sync::Arc, time::Duration};

use openmatching::{
    persister::AsyncPersister,
    protos::{Order, OrderStatus, Side, TimebasedKey},
};
use sqlx::{Pool, Postgres};
use tokio::{
    select,
    sync::{mpsc, oneshot},
};
use uuid::Uuid;

#[derive(thiserror::Error, Debug, Clone)]
pub enum PgPersisterError {
    #[error("db error")]
    DB(#[from] Arc<sqlx::Error>),
    #[error("channel closed")]
    ChannelClosed,
}

enum Command {
    Upsert {
        reply: oneshot::Sender<Result<(), PgPersisterError>>,
        orders: Vec<Order>,
    },
    Close,
}

struct Inner {
    pool: Pool<Postgres>,
    tx: mpsc::Sender<Command>,
}

impl Inner {
    async fn upsert_order(&self, orders: Vec<Order>) -> Result<(), PgPersisterError> {
        let orders = orders
            .into_iter()
            .map(|o| {
                let key: TimebasedKey = o.key.expect("key should be present").into();
                let key_uuid = uuid::Uuid::from_bytes(key.to_bytes());
                (key_uuid, o)
            })
            .collect::<BTreeMap<_, _>>();
        let (keys, (prices, quantities, remaining, sides, statuses, order_types)): (
            Vec<Uuid>,
            (Vec<f32>, Vec<i64>, Vec<i64>, Vec<i16>, Vec<i16>, Vec<i16>),
        ) = orders
            .values()
            .map(|o| {
                let key: TimebasedKey = o.key.expect("key should be present").into();
                let key_uuid = uuid::Uuid::from_bytes(key.to_bytes());
                (
                    key_uuid,
                    (
                        key.get_price(),
                        o.quantity as i64,
                        o.remaining as i64,
                        o.side as i16,
                        o.status as i16,
                        o.order_type as i16,
                    ),
                )
            })
            .unzip();
        let pool = self.pool.clone();
        let mut tx = pool.begin().await.map_err(Arc::new)?;
        let res = sqlx::query!(
                r#"
                INSERT INTO all_orders (key, price, quantity, remaining, side, status, order_type)
                SELECT * FROM unnest($1::uuid[], $2::real[], $3::bigint[], $4::bigint[], $5::smallint[], $6::smallint[], $7::smallint[])
                ON CONFLICT (key) DO UPDATE SET 
                    price = EXCLUDED.price, 
                    quantity = EXCLUDED.quantity, 
                    remaining = EXCLUDED.remaining, 
                    side = EXCLUDED.side, 
                    status = EXCLUDED.status,
                    order_type = EXCLUDED.order_type;
                "#,
                &keys,
                &prices,
                &quantities,
                &remaining,
                &sides,
                &statuses,
                &order_types,
            )
            .execute(&mut *tx)
            .await.map_err(Arc::new)?;
        tracing::debug!("upserted {} orders", res.rows_affected());
        tx.commit().await.map_err(Arc::new)?;
        Ok(())
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        while self.tx.try_send(Command::Close).is_err() && !self.tx.is_closed() {}
    }
}

#[derive(Clone)]
pub struct PgPersister {
    inner: Arc<Inner>,
}

#[derive(thiserror::Error, Debug)]
pub enum PgPersisterCreateError {
    #[error("db error")]
    DB(#[from] sqlx::Error),
    #[error("migrate error")]
    Migrate(#[from] sqlx::migrate::MigrateError),
}

impl PgPersister {
    pub async fn new(url: &str) -> Result<Self, PgPersisterCreateError> {
        let pool = Pool::connect(url).await?;
        let migrator = sqlx::migrate!("./migrations");
        migrator.run(&pool).await?;
        let (tx, mut rx) = mpsc::channel(65535);
        let inner = Arc::new(Inner { pool, tx });
        let inner_clone = inner.clone();
        tokio::spawn(async move {
            loop {
                let mut collected_orders = vec![];
                let mut collected_replys = vec![];
                let polling = async {
                    loop {
                        match rx.recv().await {
                            None | Some(Command::Close) => return true,
                            Some(Command::Upsert { reply, orders }) => {
                                collected_orders.extend(orders);
                                collected_replys.push(reply);
                            }
                        }
                        if collected_replys.len() > 256 {
                            return false;
                        }
                    }
                };
                select! {
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {},
                    is_polling_done = polling => {
                        if is_polling_done {
                            break
                        }
                    },
                }
                let res = inner_clone.upsert_order(collected_orders).await;
                for reply in collected_replys {
                    if reply.send(res.clone()).is_err() {
                        tracing::warn!("failed to send upsert reply");
                    }
                }
            }
        });
        Ok(PgPersister { inner })
    }
}

impl AsyncPersister for PgPersister {
    type Error = PgPersisterError;

    fn upsert_order(
        &self,
        orders: Vec<Order>,
    ) -> impl Future<Output = Result<(), Self::Error>> + 'static {
        let tx = self.inner.tx.clone();
        async move {
            let (reply, rx) = oneshot::channel();
            tx.send(Command::Upsert { reply, orders })
                .await
                .map_err(|_| Self::Error::ChannelClosed)?;
            rx.await.map_err(|_| Self::Error::ChannelClosed)?
        }
    }

    fn get_order(
        &self,
        key: TimebasedKey,
    ) -> impl Future<Output = Result<Option<Order>, Self::Error>> + 'static {
        let pool = self.inner.pool.clone();
        let key_uuid = uuid::Uuid::from_bytes(key.to_bytes());
        async move {
            let res = sqlx::query!(
                r#"
                SELECT * FROM all_orders WHERE key = $1
                "#,
                key_uuid,
            )
            .fetch_optional(&pool)
            .await
            .map_err(Arc::new)?;
            let Some(res) = res else {
                return Ok(None);
            };
            let key: TimebasedKey = TimebasedKey::from_bytes(res.key.into_bytes());
            let order = Order {
                key: Some(key.into()),
                quantity: res.quantity as u64,
                remaining: res.remaining as u64,
                side: res.side as i32,
                status: res.status as i32,
                order_type: res.order_type as i32, // TODO: add order type
            };
            Ok(Some(order))
        }
    }

    fn get_all_buy_orders(
        &self,
    ) -> impl Future<Output = Result<Vec<Order>, Self::Error>> + 'static {
        let pool = self.inner.pool.clone();
        async move {
            let res = sqlx::query!(
                r#"
                SELECT * FROM all_orders WHERE side = $1 and status in ($2, $3)
                "#,
                Side::Buy as i16,
                OrderStatus::Open as i16,
                OrderStatus::PartiallyFilled as i16,
            )
            .fetch_all(&pool)
            .await
            .map_err(Arc::new)?;
            let orders: Vec<Order> = res
                .iter()
                .map(|r| {
                    let key: TimebasedKey = TimebasedKey::from_bytes(r.key.into_bytes());
                    Order {
                        key: Some(key.into()),
                        quantity: r.quantity as u64,
                        remaining: r.remaining as u64,
                        side: r.side as i32,
                        status: r.status as i32,
                        order_type: r.order_type as i32,
                    }
                })
                .collect();
            Ok(orders)
        }
    }

    fn get_all_sell_orders(
        &self,
    ) -> impl Future<Output = Result<Vec<Order>, Self::Error>> + 'static {
        let pool = self.inner.pool.clone();
        async move {
            let res = sqlx::query!(
                r#"
                SELECT * FROM all_orders WHERE side = $1 and status in ($2, $3)
                "#,
                Side::Sell as i16,
                OrderStatus::Open as i16,
                OrderStatus::PartiallyFilled as i16,
            )
            .fetch_all(&pool)
            .await
            .map_err(Arc::new)?;
            let orders: Vec<Order> = res
                .iter()
                .map(|r| {
                    let key: TimebasedKey = TimebasedKey::from_bytes(r.key.into_bytes());
                    Order {
                        key: Some(key.into()),
                        quantity: r.quantity as u64,
                        remaining: r.remaining as u64,
                        side: r.side as i32,
                        status: r.status as i32,
                        order_type: r.order_type as i32,
                    }
                })
                .collect();
            Ok(orders)
        }
    }
}
