use std::{collections::BTreeMap, error::Error, marker::PhantomData, ptr::NonNull, time::Instant};

use crate::{
    persister::AsyncPersister,
    protos::{Order, OrderStatus, PricebasedKey, Side, TimebasedKey},
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

pub struct Transaction<'ob, P> {
    pub order_book: &'ob mut OrderBook<P>,
    pub updates: Vec<(&'static str, [u8; 16], Order)>,
    pub deletes: Vec<(&'static str, [u8; 16])>,
}

struct DormantMutRef<'a, T> {
    ptr: NonNull<T>,
    _marker: PhantomData<&'a T>,
}

impl<'a, T> DormantMutRef<'a, T> {
    #[allow(dead_code)]
    pub fn new(t: &'a mut T) -> (&'a mut T, Self) {
        let ptr = NonNull::from(t);
        let new_ref = unsafe { &mut *ptr.as_ptr() };
        (
            new_ref,
            Self {
                ptr,
                _marker: PhantomData,
            },
        )
    }

    pub fn new_shared(t: &'a T) -> (&'a T, Self) {
        let ptr = NonNull::from(t);
        let new_ref = unsafe { &*ptr.as_ptr() };
        (
            new_ref,
            Self {
                ptr,
                _marker: PhantomData,
            },
        )
    }

    #[allow(dead_code)]
    pub fn awaken(self) -> &'a mut T {
        unsafe { &mut *self.ptr.as_ptr() }
    }

    pub fn reborrow(&mut self) -> &'a mut T {
        unsafe { &mut *self.ptr.as_ptr() }
    }

    #[allow(dead_code)]
    pub fn reborrow_shared(&self) -> &'a T {
        unsafe { &*self.ptr.as_ptr() }
    }
}

impl<'ob, P> Transaction<'ob, P> {
    pub fn new(order_book: &'ob mut OrderBook<P>) -> Self {
        Self {
            order_book,
            updates: vec![],
            deletes: vec![],
        }
    }

    pub fn add_order(&mut self, order: Order) {
        let key: TimebasedKey = order.key.expect("key should be present").into();
        if order.side() == Side::Buy {
            self.order_book.buys.insert(key.to_pricebased(), order);
            self.updates
                .push((BUYS_CF, key.to_pricebased().to_bytes(), order));
            self.updates.push((ALL_ORDERS_CF, key.to_bytes(), order));
        } else {
            self.order_book.sells.insert(key.to_pricebased(), order);
            self.updates
                .push((SELLS_CF, key.to_pricebased().to_bytes(), order));
            self.updates.push((ALL_ORDERS_CF, key.to_bytes(), order));
        }
    }

    pub fn commit(self) -> impl Future<Output = Result<(), P::Error>> + 'static + use<P>
    where
        P: AsyncPersister<Order> + Clone,
        P: 'static + Send + Sync,
    {
        let p = self.order_book.persister.clone();
        let updates = self.updates;
        let deletes = self.deletes;
        async move {
            p.save(updates, deletes).await.inspect_err(|_| {
                tracing::error!("failed to insert transaction log");
            })?;
            Ok(())
        }
    }

    pub fn best_buy(&self) -> Option<OrderRef<'_, 'ob, P>> {
        let (_, mut dormant_transaction) = DormantMutRef::new_shared(self);
        let Some((key, order)) = dormant_transaction.reborrow().order_book.buys.pop_last() else {
            return None;
        };
        Some(OrderRef {
            dormant_tx: dormant_transaction,
            key,
            order,
        })
    }

    pub fn best_sell(&self) -> Option<OrderRef<'_, 'ob, P>> {
        let (_, mut dormant_transaction) = DormantMutRef::new_shared(self);
        let Some((key, order)) = dormant_transaction.reborrow().order_book.sells.pop_first() else {
            return None;
        };
        Some(OrderRef {
            dormant_tx: dormant_transaction,
            key,
            order,
        })
    }

    pub fn get_order_mut(&self, key: impl Into<TimebasedKey>) -> Option<OrderRef<'_, 'ob, P>> {
        let key = key.into().to_pricebased();
        let (_, mut dormant_transaction) = DormantMutRef::new_shared(self);
        if let Some(order) = dormant_transaction.reborrow().order_book.buys.remove(&key) {
            return Some(OrderRef {
                dormant_tx: dormant_transaction,
                key,
                order,
            });
        };
        if let Some(order) = dormant_transaction.reborrow().order_book.sells.remove(&key) {
            return Some(OrderRef {
                dormant_tx: dormant_transaction,
                key,
                order,
            });
        };
        None
    }

    fn run_matching(&mut self) {
        let mut last_price = self.order_book.last_price;
        {
            let Some(mut buy_ref) = self.best_buy() else {
                return;
            };
            let Some(mut sell_ref) = self.best_sell() else {
                return;
            };

            loop {
                if buy_ref.key.get_price() < sell_ref.key.get_price() {
                    break;
                }

                let quantity = buy_ref.order.remaining.min(sell_ref.order.remaining);

                buy_ref.order.remaining -= quantity;
                sell_ref.order.remaining -= quantity;
                if buy_ref.order.remaining == 0 {
                    let Some(next_buy_ref) = self.best_buy() else {
                        break;
                    };
                    buy_ref = next_buy_ref;
                }

                if sell_ref.order.remaining == 0 {
                    let Some(next_sell_ref) = self.best_sell() else {
                        break;
                    };
                    sell_ref = next_sell_ref;
                }

                last_price = buy_ref.key.get_price();
            }
        }

        self.order_book.last_price = last_price;
    }
}

pub struct OrderRef<'a, 'ob, P> {
    dormant_tx: DormantMutRef<'a, Transaction<'ob, P>>,
    pub key: PricebasedKey,
    pub order: Order,
}

impl<'a, 'ob, P> OrderRef<'a, 'ob, P> {
    pub fn cancel(mut self) {
        self.order.set_status(OrderStatus::Cancelled);
    }
}

impl<'a, 'ob, P> Drop for OrderRef<'a, 'ob, P> {
    fn drop(&mut self) {
        let cf = if self.order.side() == Side::Buy {
            BUYS_CF
        } else {
            SELLS_CF
        };

        let transaction = self.dormant_tx.reborrow();
        if self.order.status() == OrderStatus::Cancelled {
            transaction.deletes.push((cf, self.key.to_bytes()));
        } else if self.order.remaining == 0 {
            self.order.set_status(OrderStatus::Filled);
            transaction.deletes.push((cf, self.key.to_bytes()));
        } else {
            if self.order.remaining != self.order.quantity {
                self.order.set_status(OrderStatus::PartiallyFilled);
            }
            transaction
                .updates
                .push((cf, self.key.to_bytes(), self.order));
        }
        transaction
            .updates
            .push((ALL_ORDERS_CF, self.key.to_bytes(), self.order));
    }
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
    pub async fn get_order(&self, key: impl Into<TimebasedKey>) -> Result<Option<Order>, P::Error> {
        self.persister
            .load(ALL_ORDERS_CF, key.into().to_bytes())
            .await
    }
}
