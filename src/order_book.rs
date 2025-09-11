use std::{collections::BTreeMap, error::Error, time::Instant};

use crate::{
    persister::AsyncPersister,
    protos::{Order, OrderKey, OrderStatus, Side},
};

pub const BUYS_CF: &str = "buys";
pub const SELLS_CF: &str = "sells";
pub const ALL_ORDERS_CF: &str = "all_orders";

pub struct OrderBook<P> {
    pub last_price: f32,
    pub buys: BTreeMap<OrderKey, Order>,
    pub sells: BTreeMap<OrderKey, Order>,
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
    updates: Vec<(&'static str, [u8; 16], Order)>,
    deletes: Vec<(&'static str, [u8; 16])>,
}

impl<P> OrderBook<P>
where
    P: AsyncPersister<Order> + Clone,
    P: 'static + Send + Sync,
{
    #[allow(dead_code)]
    pub fn add_order(
        &mut self,
        order: Order,
    ) -> impl Future<Output = Result<(), OrderBookError<P::Error>>> + 'static + use<P> {
        let mut updates = vec![];
        let key: OrderKey = order.key.expect("key should be present").into();
        if order.side() == Side::Buy {
            self.buys.insert(key, order);
            updates.push((BUYS_CF, key.pricebased(), order));
            updates.push((ALL_ORDERS_CF, key.timebased(), order));
        } else {
            self.sells.insert(key, order);
            updates.push((SELLS_CF, key.pricebased(), order));
            updates.push((ALL_ORDERS_CF, key.timebased(), order));
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
            tracing::debug!("added order: {:?}", order);
            Ok(())
        }
    }

    #[allow(dead_code)]
    pub fn cancel_order(
        &mut self,
        key: OrderKey,
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
            let updates = vec![(ALL_ORDERS_CF, key.timebased(), order)];
            let cf = if side == Side::Buy { BUYS_CF } else { SELLS_CF };
            let deletes = vec![(cf, key.pricebased())];

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

            if buy_key.get_price() < sell_key.get_price() {
                break;
            }

            let quantity = buy.remaining.min(sell.remaining);

            buy.remaining -= quantity;
            sell.remaining -= quantity;

            if buy.remaining == 0 {
                buy.set_status(OrderStatus::Filled);
                deletes.push((BUYS_CF, buy_key.pricebased()));
            } else {
                self.buys.insert(buy_key, buy);
            }
            updates.push((ALL_ORDERS_CF, buy_key.timebased(), buy));

            if sell.remaining == 0 {
                sell.set_status(OrderStatus::Filled);
                deletes.push((SELLS_CF, sell_key.pricebased()));
            } else {
                self.sells.insert(sell_key, sell);
            }
            updates.push((ALL_ORDERS_CF, sell_key.timebased(), sell));

            self.last_price = buy_key.get_price();
        }
        MatchingResult { updates, deletes }
    }

    fn load(&mut self) -> Result<(), P::Error> {
        let now = Instant::now();
        let buys = self.persister.load_all_iter(BUYS_CF)?;
        for result in buys {
            let (key, order) = result?;
            self.buys.insert(OrderKey::from_pricebased(key), order);
        }
        println!("load {} buys in {:?}", self.buys.len(), now.elapsed());

        let now = Instant::now();
        let sells = self.persister.load_all_iter(SELLS_CF)?;
        for result in sells {
            let (key, order) = result?;
            self.sells.insert(OrderKey::from_pricebased(key), order);
        }
        println!("load {} sells in {:?}", self.sells.len(), now.elapsed());
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn create(persister: P) -> Result<Self, P::Error> {
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
    pub async fn get_order(&self, key: OrderKey) -> Result<Option<Order>, P::Error> {
        self.persister.load(ALL_ORDERS_CF, key.timebased()).await
    }
}
