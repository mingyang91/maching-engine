use std::{collections::BTreeMap, error::Error, time::Instant};

use crate::{
    persister::AsyncPersister,
    protos::{FixedKey, Order, OrderStatus, Side},
};

pub const BUYS_CF: &str = "buys";
pub const SELLS_CF: &str = "sells";
pub const ALL_ORDERS_CF: &str = "all_orders";

pub struct OrderBook<P> {
    pub last_price: f32,
    pub buys: BTreeMap<FixedKey, Order>,
    pub sells: BTreeMap<FixedKey, Order>,
    pub persister: P,
}

#[derive(thiserror::Error, Debug)]
pub enum OrderBookError<T: Error> {
    #[error("persister error")]
    Persister(#[from] T),
    #[error("failed to add order")]
    AddOrder,
    #[error("failed to cancel order")]
    CancelOrder,
}

struct MatchingResult {
    updates: Vec<(&'static str, FixedKey, Order)>,
    deletes: Vec<(&'static str, FixedKey)>,
}

impl<P> OrderBook<P>
where
    P: AsyncPersister<FixedKey, Order> + Clone,
    P: 'static + Send + Sync,
{
    #[allow(dead_code)]
    pub fn add_order(
        &mut self,
        order: Order,
    ) -> impl Future<Output = Result<(), OrderBookError<P::Error>>> + 'static {
        let mut updates = vec![];
        let key = order.key.expect("key should be present");
        if order.side() == Side::Buy {
            self.buys.insert(key.into(), order);
            updates.push((BUYS_CF, key.into(), order));
            updates.push((ALL_ORDERS_CF, key.into(), order));
        } else {
            self.sells.insert(key.into(), order);
            updates.push((SELLS_CF, key.into(), order));
            updates.push((ALL_ORDERS_CF, key.into(), order));
        }

        let MatchingResult {
            updates: updates2,
            deletes,
        } = self.run_matching();
        updates.extend(updates2);

        let persister = self.persister.clone();
        async move {
            persister
                .save(updates, deletes)
                .await
                .inspect_err(|_| {
                    tracing::error!("failed to insert add order log");
                })
                .map_err(|_| OrderBookError::AddOrder)?;
            tracing::info!("added order: {:?}", order);
            tracing::debug!("added order: {:?}", order);
            Ok(())
        }
    }

    #[allow(dead_code)]
    pub fn cancel_order(
        &mut self,
        key: FixedKey,
        side: Side,
    ) -> impl Future<Output = Result<(), OrderBookError<P::Error>>> + 'static {
        let removed = if side == Side::Buy {
            self.buys.remove(&key)
        } else {
            self.sells.remove(&key)
        };

        let persister = self.persister.clone();
        async move {
            let Some(mut order) = removed else {
                return Ok(());
            };
            order.set_status(OrderStatus::Cancelled);
            let updates = vec![(ALL_ORDERS_CF, key, order)];
            let cf = if side == Side::Buy { BUYS_CF } else { SELLS_CF };
            let deletes = vec![(cf, key)];

            persister
                .save(updates, deletes)
                .await
                .inspect_err(|_| {
                    tracing::error!("failed to insert cancel order log");
                })
                .map_err(|_| OrderBookError::CancelOrder)?;
            tracing::info!("cancelled order: {:?}", key);
            tracing::debug!("cancelled order: {:?}", order);
            Ok(())
        }
    }

    fn run_matching(&mut self) -> MatchingResult {
        let mut updates = vec![];
        let mut deletes = vec![];
        loop {
            let Some((buy_key, mut buy)) = self.buys.pop_last() else {
                break;
            };

            let Some((sell_key, mut sell)) = self.sells.pop_first() else {
                break;
            };

            if buy_key.price < sell_key.price {
                break;
            }

            let quantity = buy.remaining.min(sell.remaining);

            buy.remaining -= quantity;
            sell.remaining -= quantity;

            if buy.remaining == 0 {
                buy.set_status(OrderStatus::Filled);
                deletes.push((BUYS_CF, buy_key));
            } else {
                self.buys.insert(buy_key, buy);
            }
            updates.push((ALL_ORDERS_CF, buy_key, buy));

            if sell.remaining == 0 {
                sell.set_status(OrderStatus::Filled);
                deletes.push((SELLS_CF, sell_key));
            } else {
                self.sells.insert(sell_key, sell);
            }
            updates.push((ALL_ORDERS_CF, sell_key, sell));

            self.last_price = buy_key.price;
        }
        MatchingResult { updates, deletes }
    }

    fn load(&mut self) -> Result<(), P::Error> {
        let now = Instant::now();
        let buys = self.persister.load_all_iter(BUYS_CF)?;
        for result in buys {
            let (key, order) = result?;
            self.buys.insert(key, order);
        }
        println!("load {} buys in {:?}", self.buys.len(), now.elapsed());

        let now = Instant::now();
        let sells = self.persister.load_all_iter(SELLS_CF)?;
        for result in sells {
            let (key, order) = result?;
            self.sells.insert(key, order);
        }
        println!("load {} sells in {:?}", self.sells.len(), now.elapsed());
        Ok(())
    }

    pub fn create(persister: P) -> Result<Self, P::Error> {
        let mut order_book = Self {
            last_price: 0.0,
            buys: BTreeMap::new(),
            sells: BTreeMap::new(),
            persister,
        };
        order_book.load()?;
        Ok(order_book)
    }

    #[allow(dead_code)]
    pub async fn get_order(&self, key: FixedKey) -> Result<Option<Order>, P::Error> {
        self.persister.load(ALL_ORDERS_CF, key).await
    }
}
