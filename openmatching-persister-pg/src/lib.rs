use std::{collections::BTreeMap, sync::Arc};

use openmatching::{
    persister::AsyncPersister,
    protos::{BuyOrder, Order, SellOrder, Side, TimebasedKey, order},
};
use sqlx::{Pool, Postgres};
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct PostgresPersister {
    inner: Arc<Inner>,
}

struct Inner {
    pool: Pool<Postgres>,
    cmd_tx: mpsc::Sender<PersisteCommand>,
}

enum PersisteCommand {
    Upsert(BTreeMap<TimebasedKey, Order>),
    Abort,
}

impl PostgresPersister {
    async fn background_persist(pool: Pool<Postgres>, mut cmd_rx: mpsc::Receiver<PersisteCommand>) {
        while let Some(cmd) = cmd_rx.recv().await {
            match cmd {
                PersisteCommand::Upsert(orders) => {
                    if let Err(e) = Self::upsert_impl(pool.clone(), orders).await {
                        tracing::error!("failed to upsert orders: {e:?}");
                    }
                }
                PersisteCommand::Abort => break,
            }
        }
    }

    async fn upsert_impl(
        pool: Pool<Postgres>,
        orders: BTreeMap<TimebasedKey, Order>,
    ) -> Result<(), sqlx::Error> {
        // Separate buy and sell orders
        let mut buy_orders = Vec::new();
        let mut sell_orders = Vec::new();

        for o in orders.values() {
            let key: TimebasedKey = o.key.expect("key should be present").into();
            let key_uuid = uuid::Uuid::from_bytes(key.to_bytes());

            match &o.side_data {
                Some(order::SideData::Buy(buy)) => {
                    buy_orders.push((
                        key_uuid,
                        (
                            o.status() as i16,
                            buy.order_type() as i16,
                            buy.limit_price,
                            buy.total_funds,
                            buy.funds_remaining,
                            buy.target_quantity as i64,
                            buy.filled_quantity as i64,
                        ),
                    ));
                }
                Some(order::SideData::Sell(sell)) => {
                    sell_orders.push((
                        key_uuid,
                        (
                            o.status() as i16,
                            sell.order_type() as i16,
                            sell.limit_price,
                            sell.total_quantity as i64,
                            sell.remaining_quantity as i64,
                            sell.total_proceeds,
                        ),
                    ));
                }
                None => panic!("Order must have side_data"),
            }
        }

        let mut tx = pool.begin().await?;

        // Insert/update buy orders
        if !buy_orders.is_empty() {
            let (
                keys,
                (
                    statuses,
                    order_types,
                    limit_prices,
                    total_funds,
                    funds_remaining,
                    target_quantities,
                    filled_quantities,
                ),
            ): (
                Vec<_>,
                (Vec<_>, Vec<_>, Vec<_>, Vec<_>, Vec<_>, Vec<_>, Vec<_>),
            ) = buy_orders.into_iter().unzip();

            sqlx::query!(
                r#"
                INSERT INTO buy_orders (
                    key, status, order_type, limit_price,
                    total_funds, funds_remaining,
                    target_quantity, filled_quantity
                )
                SELECT * FROM UNNEST(
                    $1::uuid[], $2::smallint[], $3::smallint[], $4::real[],
                    $5::real[], $6::real[], $7::bigint[], $8::bigint[]
                )
                ON CONFLICT (key) DO UPDATE SET
                    status = EXCLUDED.status,
                    funds_remaining = EXCLUDED.funds_remaining,
                    filled_quantity = EXCLUDED.filled_quantity,
                    updated_at = NOW()  -- Explicitly set in query
                "#,
                &keys,
                &statuses,
                &order_types,
                &limit_prices,
                &total_funds,
                &funds_remaining,
                &target_quantities,
                &filled_quantities,
            )
            .execute(&mut *tx)
            .await?;
        }

        // Insert/update sell orders
        if !sell_orders.is_empty() {
            let (
                keys,
                (
                    statuses,
                    order_types,
                    limit_prices,
                    total_quantities,
                    remaining_quantities,
                    total_proceeds,
                ),
            ): (Vec<_>, (Vec<_>, Vec<_>, Vec<_>, Vec<_>, Vec<_>, Vec<_>)) =
                sell_orders.into_iter().unzip();

            sqlx::query!(
                r#"
                INSERT INTO sell_orders (
                    key, status, order_type, limit_price,
                    total_quantity, remaining_quantity, total_proceeds
                )
                SELECT * FROM UNNEST(
                    $1::uuid[], $2::smallint[], $3::smallint[], $4::real[],
                    $5::bigint[], $6::bigint[], $7::real[]
                )
                ON CONFLICT (key) DO UPDATE SET
                    status = EXCLUDED.status,
                    remaining_quantity = EXCLUDED.remaining_quantity,
                    total_proceeds = EXCLUDED.total_proceeds,
                    updated_at = NOW()
                "#,
                &keys,
                &statuses,
                &order_types,
                &limit_prices,
                &total_quantities,
                &remaining_quantities,
                &total_proceeds,
            )
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn new(database_url: &str) -> Result<Self, sqlx::Error> {
        let pool = Pool::<Postgres>::connect(database_url).await?;
        sqlx::migrate!().run(&pool).await?;

        let (cmd_tx, cmd_rx) = mpsc::channel(100);
        tokio::spawn(Self::background_persist(pool.clone(), cmd_rx));

        Ok(Self {
            inner: Arc::new(Inner { pool, cmd_tx }),
        })
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        while self.cmd_tx.try_send(PersisteCommand::Abort).is_err() && !self.cmd_tx.is_closed() {}
    }
}

impl AsyncPersister for PostgresPersister {
    type Error = Arc<sqlx::Error>;

    fn upsert_order(
        &self,
        orders: Vec<Order>,
    ) -> impl Future<Output = Result<(), Self::Error>> + 'static {
        let mut btree = BTreeMap::new();
        for order in orders {
            let key: TimebasedKey = order.key.expect("key should be present").into();
            btree.insert(key, order);
        }
        let cmd_tx = self.inner.cmd_tx.clone();
        async move {
            cmd_tx
                .send(PersisteCommand::Upsert(btree))
                .await
                .map_err(|_| Arc::new(sqlx::Error::PoolClosed))?;
            Ok(())
        }
    }

    fn get_order(
        &self,
        side: Side,
        key: TimebasedKey,
    ) -> impl Future<Output = Result<Option<Order>, Self::Error>> + 'static {
        let pool = self.inner.pool.clone();
        let key_uuid = uuid::Uuid::from_bytes(key.to_bytes());
        async move {
            match side {
                Side::Buy => {
                    if let Some(buy) =
                        sqlx::query!(r#"SELECT * FROM buy_orders WHERE key = $1"#, key_uuid,)
                            .fetch_optional(&pool)
                            .await
                            .map_err(Arc::new)?
                    {
                        let key: TimebasedKey = TimebasedKey::from_bytes(buy.key.into_bytes());
                        return Ok(Some(Order {
                            key: Some(key.into()),
                            status: buy.status as i32,
                            side_data: Some(order::SideData::Buy(BuyOrder {
                                order_type: buy.order_type as i32,
                                limit_price: buy.limit_price,
                                total_funds: buy.total_funds,
                                funds_remaining: buy.funds_remaining,
                                target_quantity: buy.target_quantity as u64,
                                filled_quantity: buy.filled_quantity as u64,
                            })),
                        }));
                    }
                }
                Side::Sell => {
                    if let Some(sell) =
                        sqlx::query!(r#"SELECT * FROM sell_orders WHERE key = $1"#, key_uuid,)
                            .fetch_optional(&pool)
                            .await
                            .map_err(Arc::new)?
                    {
                        let key: TimebasedKey = TimebasedKey::from_bytes(sell.key.into_bytes());
                        return Ok(Some(Order {
                            key: Some(key.into()),
                            status: sell.status as i32,
                            side_data: Some(order::SideData::Sell(SellOrder {
                                order_type: sell.order_type as i32,
                                limit_price: sell.limit_price,
                                total_quantity: sell.total_quantity as u64,
                                remaining_quantity: sell.remaining_quantity as u64,
                                total_proceeds: sell.total_proceeds,
                            })),
                        }));
                    }
                }
            }
            Ok(None)
        }
    }

    fn get_all_buy_orders(
        &self,
    ) -> impl Future<Output = Result<Vec<Order>, Self::Error>> + 'static {
        let pool = self.inner.pool.clone();
        async move {
            let res = sqlx::query!(
                r#"
                SELECT * FROM buy_orders 
                WHERE status IN (0, 1)
                ORDER BY limit_price DESC, created_at
                "#
            )
            .fetch_all(&pool)
            .await
            .map_err(Arc::new)?;

            Ok(res
                .into_iter()
                .map(|r| {
                    let key: TimebasedKey = TimebasedKey::from_bytes(r.key.into_bytes());
                    Order {
                        key: Some(key.into()),
                        status: r.status as i32,
                        side_data: Some(order::SideData::Buy(BuyOrder {
                            order_type: r.order_type as i32,
                            limit_price: r.limit_price,
                            total_funds: r.total_funds,
                            funds_remaining: r.funds_remaining,
                            target_quantity: r.target_quantity as u64,
                            filled_quantity: r.filled_quantity as u64,
                        })),
                    }
                })
                .collect())
        }
    }

    fn get_all_sell_orders(
        &self,
    ) -> impl Future<Output = Result<Vec<Order>, Self::Error>> + 'static {
        let pool = self.inner.pool.clone();
        async move {
            let res = sqlx::query!(
                r#"
                SELECT * FROM sell_orders 
                WHERE status IN (0, 1)
                ORDER BY limit_price ASC, created_at
                "#
            )
            .fetch_all(&pool)
            .await
            .map_err(Arc::new)?;

            Ok(res
                .into_iter()
                .map(|r| {
                    let key: TimebasedKey = TimebasedKey::from_bytes(r.key.into_bytes());
                    Order {
                        key: Some(key.into()),
                        status: r.status as i32,
                        side_data: Some(order::SideData::Sell(SellOrder {
                            order_type: r.order_type as i32,
                            limit_price: r.limit_price,
                            total_quantity: r.total_quantity as u64,
                            remaining_quantity: r.remaining_quantity as u64,
                            total_proceeds: r.total_proceeds,
                        })),
                    }
                })
                .collect())
        }
    }
}
