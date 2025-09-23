use super::*;

use crate::{
    borrow::DormantMutRef,
    persister::AsyncPersister,
    protos::{Order, OrderStatus, OrderType, PricebasedKey, TimebasedKey, proto_order::SideData},
};

pub struct Transaction<'ob, P> {
    pub order_book: &'ob mut OrderBook<P>,
    pub updates: Vec<Order>,
}

impl<'ob, P> Transaction<'ob, P> {
    pub(super) fn new(order_book: &'ob mut OrderBook<P>) -> Self {
        Self {
            order_book,
            updates: vec![],
        }
    }

    pub(super) fn add_order(&mut self, order: Order) {
        let key: TimebasedKey = order.key().into();
        // Add to appropriate side of order book
        match &order.side_data() {
            SideData::Buy(_) => {
                self.order_book.buys.insert(key.into(), order);
            }
            SideData::Sell(_) => {
                self.order_book.sells.insert(key.into(), order);
            }
        }
        self.updates.push(order);
    }

    pub(super) fn commit(self) -> impl Future<Output = Result<(), P::Error>> + 'static + use<P>
    where
        P: AsyncPersister + Clone,
        P: 'static + Send + Sync,
    {
        let p = self.order_book.persister.clone();
        let updates = self.updates;
        async move {
            p.upsert_order(updates).await.inspect_err(|e| {
                tracing::error!("failed to upsert orders: {e:?}");
            })?;
            Ok(())
        }
    }

    pub(super) fn best_buy(&self) -> Option<OrderRef<'_, 'ob, P>> {
        let (_, mut dormant_transaction) = DormantMutRef::new_shared(self);
        let (key, order) = dormant_transaction.reborrow().order_book.buys.pop_last()?;
        Some(OrderRef {
            dormant_tx: dormant_transaction,
            key,
            order,
        })
    }

    pub(super) fn best_sell(&self) -> Option<OrderRef<'_, 'ob, P>> {
        let (_, mut dormant_transaction) = DormantMutRef::new_shared(self);
        let (key, order) = dormant_transaction
            .reborrow()
            .order_book
            .sells
            .pop_first()?;
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

    /// Clean matching for separated Buy/Sell order types
    pub(super) fn run_matching(&mut self) {
        let mut last_price = self.order_book.last_price;

        loop {
            // Get best orders from each side
            let Some(mut buy_ref) = self.best_buy() else {
                break;
            };
            let Some(mut sell_ref) = self.best_sell() else {
                drop(buy_ref);
                break;
            };

            // Extract buy and sell data
            let SideData::Buy(buy_data) = &buy_ref.order.side_data() else {
                unreachable!("Buy side should have buy data");
            };
            let SideData::Sell(sell_data) = &sell_ref.order.side_data() else {
                unreachable!("Sell side should have sell data");
            };

            // Determine if orders can match
            let can_match = match (buy_data.order_type(), sell_data.order_type()) {
                // Market orders always match
                (OrderType::Market, _) | (_, OrderType::Market) => true,
                // Limit orders match when buy limit >= sell limit
                (OrderType::Limit, OrderType::Limit) => {
                    buy_data.limit_price >= sell_data.limit_price
                }
            };

            if !can_match {
                break;
            }

            // Determine execution price (maker's price)
            let execution_price = match (buy_data.order_type(), sell_data.order_type()) {
                (OrderType::Market, _) => sell_data.limit_price,
                (_, OrderType::Market) => buy_data.limit_price,
                _ => {
                    // Both limits - use maker's price (first in book)
                    if buy_ref.key.to_timebased() < sell_ref.key.to_timebased() {
                        buy_data.limit_price
                    } else {
                        sell_data.limit_price
                    }
                }
            };

            // Calculate maximum executable quantity
            // Limited by: buyer's funds, seller's shares, buyer's target
            let max_qty_by_funds = if execution_price > 0.0 {
                (buy_data.funds_remaining / execution_price).floor() as u64
            } else {
                u64::MAX
            };

            let quantity = max_qty_by_funds
                .min(sell_data.remaining_quantity)
                .min(buy_data.target_quantity - buy_data.filled_quantity);

            if quantity == 0 {
                // Can't execute - buyer out of funds or target reached
                drop(buy_ref);
                drop(sell_ref);
                break;
            }

            let trade_value = quantity as f32 * execution_price;

            // Update buy order
            if let SideData::Buy(ref mut buy_mut) = buy_ref.order.side_data() {
                buy_mut.funds_remaining -= trade_value;
                buy_mut.filled_quantity += quantity;

                // Check if buy order is complete
                if buy_mut.filled_quantity >= buy_mut.target_quantity
                    || buy_mut.funds_remaining < 0.01
                {
                    buy_ref.order.set_status(OrderStatus::Filled);
                } else {
                    buy_ref.order.set_status(OrderStatus::PartiallyFilled);
                }
            }

            // Update sell order
            if let SideData::Sell(ref mut sell_mut) = sell_ref.order.side_data() {
                sell_mut.remaining_quantity -= quantity;
                sell_mut.total_proceeds += trade_value;

                // Check if sell order is complete
                if sell_mut.remaining_quantity == 0 {
                    sell_ref.order.set_status(OrderStatus::Filled);
                } else {
                    sell_ref.order.set_status(OrderStatus::PartiallyFilled);
                }
            }

            last_price = execution_price;
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
        let transaction = self.dormant_tx.reborrow();
        transaction.updates.push(self.order);
    }
}
