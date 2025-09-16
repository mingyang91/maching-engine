use std::{collections::BTreeSet, sync::Arc};

use openmatching::{
    persister::AsyncPersister,
    protos::{Order, OrderStatus, Side, TimebasedKey},
};
use sqlx::{Pool, Postgres};
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(thiserror::Error, Debug)]
pub enum PgPersisterError {
    #[error("db error")]
    DB(#[from] sqlx::Error),
    #[error("migrate error")]
    Migrate(#[from] sqlx::migrate::MigrateError),
}

#[derive(Clone)]
pub struct PgPersister {
    pool: Arc<Mutex<Pool<Postgres>>>,
}

impl PgPersister {
    pub async fn new(url: &str) -> Result<Self, PgPersisterError> {
        let pool = Pool::connect(url).await?;
        let migrator = sqlx::migrate!("./migrations");
        migrator.run(&pool).await?;
        Ok(PgPersister {
            pool: Arc::new(Mutex::new(pool)),
        })
    }
}

// fn order_to_upsert(orders: Vec<Order>) -> Vec<String> {
//     let mut all_orders = vec![
//         "INSERT INTO all_orders (key, price, quantity, remaining, side, status) VALUES\n"
//             .to_string(),
//     ];

//     for order in orders {
//         let key: TimebasedKey = order.key.expect("key should be present").into();
//         let key_uuid = uuid::Uuid::from_bytes(key.to_bytes());
//         let values = format!(
//             "('{key}', {price}, {quantity}, {remaining}, '{side}', '{status}'),\n",
//             key = key_uuid,
//             price = key.get_price(),
//             quantity = order.quantity,
//             remaining = order.remaining,
//             side = order.side,
//             status = order.status
//         );

//         all_orders.push(values);
//     }

//     let on_conflict = r#"ON CONFLICT (key) DO UPDATE SET
//         price = EXCLUDED.price,
//         quantity = EXCLUDED.quantity,
//         remaining = EXCLUDED.remaining,
//         side = EXCLUDED.side,
//         status = EXCLUDED.status;
//         "#
//     .to_string();
// }

impl AsyncPersister for PgPersister {
    type Error = PgPersisterError;

    fn upsert_order(
        &self,
        orders: Vec<Order>,
    ) -> impl Future<Output = Result<(), Self::Error>> + 'static {
        let mut dedup_keys = BTreeSet::new();
        let (keys, (prices, quantities, remaining, sides, statuses, order_types)): (
            Vec<Uuid>,
            (Vec<f32>, Vec<i64>, Vec<i64>, Vec<i16>, Vec<i16>, Vec<i16>),
        ) = orders
            .iter()
            .filter(|o| dedup_keys.insert(o.key.expect("key should be present")))
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
        async move {
            let pool = pool.lock().await;
            let mut tx = pool.begin().await?;
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
            .await?;
            tracing::debug!("upserted {} orders", res.rows_affected());
            tx.commit().await?;
            Ok(())
        }
    }

    fn get_order(
        &self,
        key: TimebasedKey,
    ) -> impl Future<Output = Result<Option<Order>, Self::Error>> + 'static {
        let pool = self.pool.clone();
        let key_uuid = uuid::Uuid::from_bytes(key.to_bytes());
        async move {
            let pool = pool.lock().await;
            let res = sqlx::query!(
                r#"
                SELECT * FROM all_orders WHERE key = $1
                "#,
                key_uuid,
            )
            .fetch_optional(&*pool)
            .await?;
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
        let pool = self.pool.clone();
        async move {
            let pool = pool.lock().await;
            let res = sqlx::query!(
                r#"
                SELECT * FROM all_orders WHERE side = $1 and status in ($2, $3)
                "#,
                Side::Buy as i16,
                OrderStatus::Open as i16,
                OrderStatus::PartiallyFilled as i16,
            )
            .fetch_all(&*pool)
            .await?;
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
        let pool = self.pool.clone();
        async move {
            let pool = pool.lock().await;
            let res = sqlx::query!(
                r#"
                SELECT * FROM all_orders WHERE side = $1 and status in ($2, $3)
                "#,
                Side::Sell as i16,
                OrderStatus::Open as i16,
                OrderStatus::PartiallyFilled as i16,
            )
            .fetch_all(&*pool)
            .await?;
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
