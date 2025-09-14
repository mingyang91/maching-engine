use super::*;

use crate::{
    borrow::DormantMutRef,
    persister::AsyncPersister,
    protos::{Order, OrderStatus, PricebasedKey, Side, TimebasedKey},
};

pub struct Transaction<'ob, P> {
    pub order_book: &'ob mut OrderBook<P>,
    pub updates: Vec<(&'static str, [u8; 16], Order)>,
    pub deletes: Vec<(&'static str, [u8; 16])>,
}

impl<'ob, P> Transaction<'ob, P> {
    pub(super) fn new(order_book: &'ob mut OrderBook<P>) -> Self {
        Self {
            order_book,
            updates: vec![],
            deletes: vec![],
        }
    }

    pub(super) fn add_order(&mut self, order: Order) {
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

    pub(super) fn commit(self) -> impl Future<Output = Result<(), P::Error>> + 'static + use<P>
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

    pub(super) fn best_buy(&self) -> Option<OrderRef<'_, 'ob, P>> {
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

    pub(super) fn best_sell(&self) -> Option<OrderRef<'_, 'ob, P>> {
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

    pub(super) fn get_order_mut(
        &self,
        key: impl Into<TimebasedKey>,
    ) -> Option<OrderRef<'_, 'ob, P>> {
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

    pub(super) fn run_matching(&mut self) {
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
