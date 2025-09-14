mod transaction;

use std::{collections::BTreeMap, error::Error, time::Instant};

use crate::{
    order_book::transaction::Transaction,
    persister::AsyncPersister,
    protos::{Order, PricebasedKey, Side, TimebasedKey},
};

pub const BUYS_CF: &str = "buys";
pub const SELLS_CF: &str = "sells";
pub const ALL_ORDERS_CF: &str = "all_orders";

pub struct OrderBook<P> {
    pub last_price: f32,
    pub buys: BTreeMap<PricebasedKey, Order>,
    pub sells: BTreeMap<PricebasedKey, Order>,
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

impl<P> OrderBook<P>
where
    P: AsyncPersister<Order> + Clone,
    P: 'static + Send + Sync,
{
    fn begin_transaction<'a>(&'a mut self) -> Transaction<'a, P> {
        Transaction::new(self)
    }

    #[allow(dead_code)]
    pub fn add_order(
        &mut self,
        order: Order,
    ) -> impl Future<Output = Result<(), OrderBookError<P::Error>>> + 'static + use<P> {
        let mut transaction = self.begin_transaction();
        transaction.add_order(order);
        transaction.run_matching();
        let fut = transaction.commit();
        async move {
            fut.await.map_err(|_| OrderBookError::AddOrder)?;
            Ok(())
        }
    }

    #[allow(dead_code)]
    pub fn cancel_order(
        &mut self,
        key: impl Into<TimebasedKey> + Copy + 'static,
        _side: Side,
    ) -> impl Future<Output = Result<(), OrderBookError<P::Error>>> + 'static {
        let transaction = self.begin_transaction();
        if let Some(order) = transaction.get_order_mut(key) {
            order.cancel();
        };
        let fut = transaction.commit();
        async move {
            fut.await.map_err(|_| OrderBookError::CancelOrder)?;
            Ok(())
        }
    }

    fn load(&mut self) -> Result<(), P::Error> {
        let now = Instant::now();
        let buys = self.persister.load_all_iter(BUYS_CF)?;
        for result in buys {
            let (key, order) = result?;
            self.buys.insert(PricebasedKey::from_bytes(key), order);
        }
        tracing::info!("load {} buys in {:?}", self.buys.len(), now.elapsed());

        let now = Instant::now();
        let sells = self.persister.load_all_iter(SELLS_CF)?;
        for result in sells {
            let (key, order) = result?;
            self.sells.insert(PricebasedKey::from_bytes(key), order);
        }
        tracing::info!("load {} sells in {:?}", self.sells.len(), now.elapsed());
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
    pub async fn get_order(&self, key: impl Into<TimebasedKey>) -> Result<Option<Order>, P::Error> {
        self.persister
            .load(ALL_ORDERS_CF, key.into().to_bytes())
            .await
    }
}
