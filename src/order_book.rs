use std::{collections::BTreeMap, error::Error};

use rocksdb::DB;
use tokio::task::yield_now;

use crate::{
    logs::{LogsPersisterError, WAL},
    persister::PersisterError,
    protos::{Execution, Key, Log, Order, OrderStatus, Side, log},
};

pub struct OrderBook<W> {
    pub last_sequence: u64,
    pub last_price: f32,
    pub buys: BTreeMap<Key, Order>,
    pub sells: BTreeMap<Key, Order>,
    pub wal: W,
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

fn add_order_log(seq: u64, order: Order) -> Log {
    Log {
        sequence: seq,
        kind: Some(log::Kind::AddOrder(log::AddOrder { order: Some(order) })),
    }
}

fn cancel_order_log(seq: u64, key: Key) -> Log {
    Log {
        sequence: seq,
        kind: Some(log::Kind::CancelOrder(log::CancelOrder { key: Some(key) })),
    }
}

fn execution_log(seq: u64, execution: Execution) -> Log {
    Log {
        sequence: seq,
        kind: Some(log::Kind::OrderExecution(log::OrderExecution {
            execution: Some(execution),
        })),
    }
}

impl OrderBook<WAL<DB, PersisterError<rocksdb::Error>>> {
    pub fn add_order(
        &mut self,
        order: Order,
    ) -> impl Future<Output = Result<(), OrderBookError<LogsPersisterError<rocksdb::Error>>>> + 'static
    {
        let key = order.key.expect("key should be present");
        if order.side() == Side::Buy {
            self.buys.insert(key, order);
        } else {
            self.sells.insert(key, order);
        }

        self.last_sequence += 1;
        let mut logs = vec![add_order_log(self.last_sequence, order)];
        let executions = self.run_matching();

        for execution in executions {
            self.last_sequence += 1;
            logs.push(execution_log(self.last_sequence, execution));
        }
        let wal = self.wal.clone();
        async move {
            yield_now().await;
            wal.insert_batch(logs)
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
    ) -> impl Future<Output = Result<(), OrderBookError<LogsPersisterError<rocksdb::Error>>>> + 'static
    {
        let removed = if side == Side::Buy {
            self.buys.remove(&key)
        } else {
            self.sells.remove(&key)
        };

        self.last_sequence += 1;
        let last_sequence = self.last_sequence;
        let logs = vec![cancel_order_log(last_sequence, key)];
        let wal = self.wal.clone();
        async move {
            yield_now().await;
            let Some(order) = removed else {
                return Ok(());
            };

            wal.insert_batch(logs)
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

    fn run_matching(&mut self) -> Vec<Execution> {
        let mut executions = vec![];
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
            } else {
                self.buys.insert(buy_key, buy);
            }

            if sell.remaining == 0 {
                sell.set_status(OrderStatus::Filled);
            } else {
                self.sells.insert(sell_key, sell);
            }

            let execution = Execution {
                buyer: Some(buy),
                seller: Some(sell),
                quantity: buy.quantity.min(sell.quantity),
            };
            executions.push(execution);
            self.last_price = buy_key.price;
        }
        executions
    }
}
