use std::{
    collections::BTreeMap,
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};

use rocksdb::{DB, Options};

use crate::{
    order_book::OrderBook,
    persister::{Persister, PersisterError},
    protos::{Key, Log, Order, Side, log},
};

#[derive(thiserror::Error, Debug)]
pub enum OrderBookError<T: Error> {
    #[error("persister error")]
    PersisterError(#[from] T),
}

struct OrderBookPersisterInner<P> {
    persister: P,
    alive: AtomicBool,
}

// #[derive(Clone)]
pub struct OrderBookPersister<P>
where
    P: Persister<Key, Order>,
    P: 'static + Send + Sync,
{
    inner: Arc<OrderBookPersisterInner<P>>,
}

impl<P> Clone for OrderBookPersister<P>
where
    P: Persister<Key, Order>,
    P: 'static + Send + Sync,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<P> OrderBookPersister<P>
where
    P: Persister<Key, Order>,
    P: 'static + Send + Sync,
{
    pub fn load(persister: P) -> Result<Self, OrderBookError<P::Error>> {
        let order_book_persister = OrderBookPersisterInner {
            persister,
            alive: AtomicBool::new(true),
        };
        let order_book_persister = OrderBookPersister {
            inner: Arc::new(order_book_persister),
        };
        order_book_persister.sync_to_latest()?;

        Ok(order_book_persister)
    }

    fn sync_forever(&self) -> Result<(), OrderBookError<P::Error>> {
        while self.inner.alive.load(Ordering::Relaxed) {
            let len = self.sync_once().expect("failed to sync");
            if len == 0 {
                thread::yield_now();
            } else {
                tracing::info!("synced {} logs", len);
            }
        }
        tracing::info!("sync forever stopped");
        Ok(())
    }

    pub fn sync_forever_thread(&self) {
        let bind: Self = self.clone();
        thread::spawn(move || {
            bind.sync_forever().expect("failed to sync");
        });
    }

    fn stop(&self) {
        self.inner.alive.store(false, Ordering::Relaxed);
    }

    fn save(&self, logs: Vec<Log>) -> Result<(), OrderBookError<P::Error>> {
        let mut updates = vec![];
        let mut deletes = vec![];
        for log in logs {
            let Some(kind) = log.kind else { continue };
            match kind {
                log::Kind::AddOrder(add_order) => {
                    let order = add_order.order.expect("order should be present");
                    let key = order.key.expect("key should be present");
                    updates.push((key, order));
                }
                log::Kind::CancelOrder(cancel_order) => {
                    let key = cancel_order.key.expect("key should be present");
                    deletes.push(key);
                }
                log::Kind::OrderExecution(order_execution) => {
                    let execution = order_execution
                        .execution
                        .expect("execution should be present");
                    let buyer = execution.buyer.expect("buyer should be present");
                    let seller = execution.seller.expect("seller should be present");
                    let buyer_key = buyer.key.expect("buyer key should be present");
                    let seller_key = seller.key.expect("seller key should be present");
                    if buyer.remaining == 0 {
                        deletes.push(buyer_key);
                    } else {
                        updates.push((buyer_key, buyer));
                    }
                    if seller.remaining == 0 {
                        deletes.push(seller_key);
                    } else {
                        updates.push((seller_key, seller));
                    }
                }
            }
        }
        self.inner.persister.save_batch(updates, deletes)?;
        Ok(())
    }

    pub fn load_order_book(&self) -> Result<OrderBook<WAL<P, P::Error>>, OrderBookError<P::Error>> {
        let last_sequence = self.inner.persister.last_sequence()?;
        let mut buys = BTreeMap::new();
        let mut sells = BTreeMap::new();

        for result in self.inner.persister.load_prefix_iter(b"o")? {
            let (key, order) = result?;
            if order.side() == Side::Buy {
                buys.insert(key, order);
            } else {
                sells.insert(key, order);
            }
        }

        Ok(OrderBook {
            last_sequence,
            last_price: 0.0, // TODO: load from persister
            buys,
            sells,
        })
    }
}

impl OrderBookPersister<DB> {
    pub fn init(path: &str) -> Result<Self, PersisterError<rocksdb::Error>> {
        let mut options = Options::default();
        options.create_if_missing(true);
        let persister = DB::open(&options, path).expect("failed to open db");
        let order_book_persister = OrderBookPersister {
            inner: Arc::new(OrderBookPersisterInner {
                persister,
                alive: AtomicBool::new(true),
            }),
        };
        order_book_persister.sync_forever_thread();
        Ok(order_book_persister)
    }
}
