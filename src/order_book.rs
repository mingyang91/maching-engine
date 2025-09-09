use std::{collections::BTreeMap, error::Error};

use tokio::task::yield_now;

use crate::{
    persister::AsyncPersister,
    protos::{Key, Order, OrderStatus, Side},
};

pub const ORDER_BOOK_CF: &str = "order_book";
pub const ALL_ORDERS_CF: &str = "all_orders";

pub struct OrderBook<P> {
    pub last_sequence: u64,
    pub last_price: f32,
    pub buys: BTreeMap<Key, Order>,
    pub sells: BTreeMap<Key, Order>,
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
    P: AsyncPersister<Key, Order> + Clone,
    P: 'static + Send + Sync,
{
    pub fn add_order(
        &mut self,
        order: Order,
    ) -> impl Future<Output = Result<(), OrderBookError<P::Error>>> + 'static {
        let key = order.key.expect("key should be present");
        if order.side() == Side::Buy {
            self.buys.insert(key, order);
        } else {
            self.sells.insert(key, order);
        }

        let (mut updates, deletes) = self.run_matching();
        updates.push((ORDER_BOOK_CF, key, order));

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
            println!("added order: {:?}", order);
            tracing::info!("added order: {:?}", order);
            tracing::debug!("added order: {:?}", order);
            Ok(())
        }
    }

    pub fn cancel_order(
        &mut self,
        key: Key,
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
            let deletes = vec![(ORDER_BOOK_CF, key)];

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

    fn run_matching(&mut self) -> (Vec<(&'static str, Key, Order)>, Vec<(&'static str, Key)>) {
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
                deletes.push((ORDER_BOOK_CF, buy_key));
            } else {
                self.buys.insert(buy_key, buy);
            }
            updates.push((ALL_ORDERS_CF, buy_key, buy));

            if sell.remaining == 0 {
                sell.set_status(OrderStatus::Filled);
                deletes.push((ORDER_BOOK_CF, sell_key));
            } else {
                self.sells.insert(sell_key, sell);
            }
            updates.push((ALL_ORDERS_CF, sell_key, sell));

            self.last_price = buy_key.price;
        }
        (updates, deletes)
    }

    fn load(&mut self) -> Result<(), P::Error> {
        let orders = self.persister.load_all_iter(ORDER_BOOK_CF)?;
        for result in orders {
            let (key, order) = result?;
            if order.side() == Side::Buy {
                self.buys.insert(key, order);
            } else {
                self.sells.insert(key, order);
            }
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

    pub async fn get_order(&self, key: Key) -> Result<Option<Order>, P::Error> {
        self.persister.load(ALL_ORDERS_CF, key).await
    }
}
