use std::{collections::BTreeMap, error::Error};

use tokio::task::yield_now;

use crate::{
    persister::AsyncPersister,
    protos::{FixedKey, Order, OrderStatus, Side},
};

pub const BUYS_CF: &str = "buys";
pub const SELLS_CF: &str = "sells";
pub const ALL_ORDERS_CF: &str = "all_orders";

pub struct OrderBook<P> {
    pub last_sequence: u64,
    pub last_price: f32,
    pub buys: BTreeMap<FixedKey, Order>,
    pub sells: BTreeMap<FixedKey, Order>,
    pub persister: P,
}

#[derive(thiserror::Error, Debug)]
pub enum OrderBookError<T: Error> {
    #[error("persister error")]
    PersisterError(#[from] T),
    #[error("failed to add order")]
    AddOrderError,
    #[error("failed to cancel order")]
    CancelOrderError,
}

impl<P> OrderBook<P>
where
    P: AsyncPersister<FixedKey, Order> + Clone,
    P: 'static + Send + Sync,
{
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

        let (updates2, deletes) = self.run_matching();
        updates.extend(updates2);

        let persister = self.persister.clone();
        async move {
            yield_now().await;
            persister
                .save(updates, deletes)
                .await
                .inspect_err(|_| {
                    tracing::error!("failed to insert add order log");
                })
                .map_err(|_| OrderBookError::AddOrderError)?;
            tracing::info!("added order: {:?}", order);
            tracing::debug!("added order: {:?}", order);
            Ok(())
        }
    }

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
            yield_now().await;
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
                .map_err(|_| OrderBookError::CancelOrderError)?;
            tracing::info!("cancelled order: {:?}", key);
            tracing::debug!("cancelled order: {:?}", order);
            Ok(())
        }
    }

    fn run_matching(
        &mut self,
    ) -> (
        Vec<(&'static str, FixedKey, Order)>,
        Vec<(&'static str, FixedKey)>,
    ) {
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
        (updates, deletes)
    }

    fn load(&mut self) -> Result<(), P::Error> {
        let buys = self.persister.load_all_iter(BUYS_CF)?;
        for result in buys {
            let (key, order) = result?;
            self.buys.insert(key.into(), order);
        }

        let sells = self.persister.load_all_iter(SELLS_CF)?;
        for result in sells {
            let (key, order) = result?;
            self.sells.insert(key.into(), order);
        }
        Ok(())
    }

    pub fn create(persister: P) -> Result<Self, P::Error> {
        let mut order_book = Self {
            last_sequence: 0,
            last_price: 0.0,
            buys: BTreeMap::new(),
            sells: BTreeMap::new(),
            persister,
        };
        order_book.load()?;
        Ok(order_book)
    }

    pub async fn get_order(&self, key: FixedKey) -> Result<Option<Order>, P::Error> {
        self.persister.load(ALL_ORDERS_CF, key).await
    }
}
