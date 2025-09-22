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
        let mut pending_orders = BTreeMap::new();
        let mut flush_interval = tokio::time::interval(std::time::Duration::from_millis(10));

        loop {
            tokio::select! {
                Some(cmd) = cmd_rx.recv() => {
                    match cmd {
                        PersisteCommand::Upsert(orders) => {
                            // Accumulate orders for batching
                            pending_orders.extend(orders);

                            // Flush if batch is large enough
                            if pending_orders.len() >= 1000 {
                                if let Err(e) = Self::upsert_impl(pool.clone(), std::mem::take(&mut pending_orders)).await {
                                    tracing::error!("failed to upsert orders: {e:?}");
                                }
                            }
                        }
                        PersisteCommand::Abort => break,
                    }
                }
                _ = flush_interval.tick() => {
                    // Periodic flush for low-volume periods
                    if !pending_orders.is_empty() {
                        if let Err(e) = Self::upsert_impl(pool.clone(), std::mem::take(&mut pending_orders)).await {
                            tracing::error!("failed to upsert orders: {e:?}");
                        }
                    }
                }
            }
        }

        // Flush any remaining orders before exit
        if !pending_orders.is_empty() {
            let _ = Self::upsert_impl(pool.clone(), pending_orders).await;
        }
    }

    async fn upsert_impl(
        pool: Pool<Postgres>,
        orders: BTreeMap<TimebasedKey, Order>,
    ) -> Result<(), sqlx::Error> {
        // Prepare data for single atomic insert
        let mut keys = Vec::new();
        let mut sides = Vec::new();
        let mut statuses = Vec::new();
        let mut order_types = Vec::new();
        let mut limit_prices = Vec::new();

        // Buy-specific (nullable for sells)
        let mut total_funds = Vec::new();
        let mut funds_remaining = Vec::new();
        let mut target_quantities = Vec::new();
        let mut filled_quantities = Vec::new();

        // Sell-specific (nullable for buys)
        let mut total_quantities = Vec::new();
        let mut remaining_quantities = Vec::new();
        let mut total_proceeds = Vec::new();

        for o in orders.values() {
            let key: TimebasedKey = o.key.expect("key should be present").into();
            let key_uuid = uuid::Uuid::from_bytes(key.to_bytes());
            keys.push(key_uuid);
            statuses.push(o.status() as i16);

            match &o.side_data {
                Some(order::SideData::Buy(buy)) => {
                    sides.push(0i16); // BUY
                    order_types.push(buy.order_type() as i16);
                    limit_prices.push(buy.limit_price);

                    // Buy fields
                    total_funds.push(Some(buy.total_funds));
                    funds_remaining.push(Some(buy.funds_remaining));
                    target_quantities.push(Some(buy.target_quantity as i64));
                    filled_quantities.push(Some(buy.filled_quantity as i64));

                    // Sell fields are NULL
                    total_quantities.push(None);
                    remaining_quantities.push(None);
                    total_proceeds.push(None);
                }
                Some(order::SideData::Sell(sell)) => {
                    sides.push(1i16); // SELL
                    order_types.push(sell.order_type() as i16);
                    limit_prices.push(sell.limit_price);

                    // Buy fields are NULL
                    total_funds.push(None);
                    funds_remaining.push(None);
                    target_quantities.push(None);
                    filled_quantities.push(None);

                    // Sell fields
                    total_quantities.push(Some(sell.total_quantity as i64));
                    remaining_quantities.push(Some(sell.remaining_quantity as i64));
                    total_proceeds.push(Some(sell.total_proceeds));
                }
                None => panic!("Order must have side_data"),
            }
        }

        // Single atomic transaction for all orders
        let mut tx = pool.begin().await?;

        sqlx::query!(
            r#"
            INSERT INTO orders (
                key, side, status, order_type, limit_price,
                total_funds, funds_remaining, target_quantity, filled_quantity,
                total_quantity, remaining_quantity, total_proceeds
            )
            SELECT * FROM UNNEST(
                $1::uuid[], $2::smallint[], $3::smallint[], $4::smallint[], $5::real[],
                $6::real[], $7::real[], $8::bigint[], $9::bigint[],
                $10::bigint[], $11::bigint[], $12::real[]
            )
            ON CONFLICT (key) DO UPDATE SET
                status = EXCLUDED.status,
                funds_remaining = EXCLUDED.funds_remaining,
                filled_quantity = EXCLUDED.filled_quantity,
                remaining_quantity = EXCLUDED.remaining_quantity,
                total_proceeds = EXCLUDED.total_proceeds,
                updated_at = NOW()
            "#,
            &keys,
            &sides,
            &statuses,
            &order_types,
            &limit_prices,
            &total_funds as &[Option<f32>],
            &funds_remaining as &[Option<f32>],
            &target_quantities as &[Option<i64>],
            &filled_quantities as &[Option<i64>],
            &total_quantities as &[Option<i64>],
            &remaining_quantities as &[Option<i64>],
            &total_proceeds as &[Option<f32>],
        )
        .execute(&mut *tx)
        .await?;

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
        _side: Side,
        key: TimebasedKey,
    ) -> impl Future<Output = Result<Option<Order>, Self::Error>> + 'static {
        let pool = self.inner.pool.clone();
        let key_uuid = uuid::Uuid::from_bytes(key.to_bytes());
        async move {
            let res = sqlx::query!(r#"SELECT * FROM orders WHERE key = $1"#, key_uuid,)
                .fetch_optional(&pool)
                .await
                .map_err(Arc::new)?;

            let Some(res) = res else {
                return Ok(None);
            };

            let key: TimebasedKey = TimebasedKey::from_bytes(res.key.into_bytes());

            // Reconstruct order based on side
            let side_data = if res.side == 0 {
                // BUY
                Some(order::SideData::Buy(BuyOrder {
                    order_type: res.order_type as i32,
                    limit_price: res.limit_price,
                    total_funds: res.total_funds.unwrap_or(0.0),
                    funds_remaining: res.funds_remaining.unwrap_or(0.0),
                    target_quantity: res.target_quantity.unwrap_or(0) as u64,
                    filled_quantity: res.filled_quantity.unwrap_or(0) as u64,
                }))
            } else {
                // SELL
                Some(order::SideData::Sell(SellOrder {
                    order_type: res.order_type as i32,
                    limit_price: res.limit_price,
                    total_quantity: res.total_quantity.unwrap_or(0) as u64,
                    remaining_quantity: res.remaining_quantity.unwrap_or(0) as u64,
                    total_proceeds: res.total_proceeds.unwrap_or(0.0),
                }))
            };

            Ok(Some(Order {
                key: Some(key.into()),
                status: res.status as i32,
                side_data,
            }))
        }
    }

    fn get_all_buy_orders(
        &self,
    ) -> impl Future<Output = Result<Vec<Order>, Self::Error>> + 'static {
        let pool = self.inner.pool.clone();
        async move {
            let res = sqlx::query!(
                r#"
                SELECT * FROM orders 
                WHERE side = 0 AND status IN (0, 1)
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
                            total_funds: r.total_funds.unwrap_or(0.0),
                            funds_remaining: r.funds_remaining.unwrap_or(0.0),
                            target_quantity: r.target_quantity.unwrap_or(0) as u64,
                            filled_quantity: r.filled_quantity.unwrap_or(0) as u64,
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
                SELECT * FROM orders 
                WHERE side = 1 AND status IN (0, 1)
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
                            total_quantity: r.total_quantity.unwrap_or(0) as u64,
                            remaining_quantity: r.remaining_quantity.unwrap_or(0) as u64,
                            total_proceeds: r.total_proceeds.unwrap_or(0.0),
                        })),
                    }
                })
                .collect())
        }
    }
}
