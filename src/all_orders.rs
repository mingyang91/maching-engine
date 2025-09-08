use std::{
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};

use rocksdb::{DB, Options};

use crate::{
    logs::WAL,
    persister::{MetadataStore, Persister, PersisterError},
    protos::{Key, Log, Order, OrderStatus, log},
};

struct Inner<P, W> {
    persister: P,
    wal: W,
    alive: AtomicBool,
}

pub struct AllOrders<OP, LP, E>
where
    OP: Persister<Key, Order, Error = E>,
    OP: MetadataStore<Error = E>,
    OP: 'static + Send + Sync,
    LP: Persister<u64, Log, Error = E>,
    LP: MetadataStore<Error = E>,
    LP: 'static + Send + Sync,
    E: Error + 'static,
{
    inner: Arc<Inner<OP, WAL<LP, E>>>,
}

impl<OP, LP, E> Clone for AllOrders<OP, LP, E>
where
    OP: Persister<Key, Order, Error = E>,
    OP: MetadataStore<Error = E>,
    OP: 'static + Send + Sync,
    LP: Persister<u64, Log, Error = E>,
    LP: MetadataStore<Error = E>,
    LP: 'static + Send + Sync,
    E: Error + 'static,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<OP, LP, E> AllOrders<OP, LP, E>
where
    OP: Persister<Key, Order, Error = E>,
    OP: MetadataStore<Error = E>,
    OP: 'static + Send + Sync,
    LP: Persister<u64, Log, Error = E>,
    LP: MetadataStore<Error = E>,
    LP: 'static + Send + Sync,
    E: Error + 'static,
{
    fn sync_forever(&self) -> Result<(), E> {
        while self.inner.alive.load(Ordering::Relaxed) {
            let last_sequence = self.inner.persister.last_sequence()?;
            let wal_last_sequence = self.inner.wal.last_sequence();
            if last_sequence == wal_last_sequence {
                thread::yield_now();
                continue;
            }

            let logs = self.inner.wal.fetch_logs_by_sequence(last_sequence, 1024)?;
            if logs.is_empty() {
                thread::yield_now();
                continue;
            }
            self.save(logs)?;
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

    fn save(&self, logs: Vec<Log>) -> Result<(), E> {
        let mut updates = vec![];
        for log in logs {
            let Some(kind) = log.kind else {
                tracing::error!("log kind should be present");
                continue;
            };
            match kind {
                log::Kind::AddOrder(add_order) => {
                    let order = add_order.order.expect("order should be present");
                    let key = order.key.expect("key should be present");
                    updates.push((key, order));
                }
                log::Kind::CancelOrder(cancel_order) => {
                    let key = cancel_order.key.expect("key should be present");
                    let mut order = self
                        .inner
                        .persister
                        .load(key)?
                        .expect("order should be present");
                    order.set_status(OrderStatus::Cancelled);
                    updates.push((key, order));
                }
                log::Kind::OrderExecution(order_execution) => {
                    let execution = order_execution
                        .execution
                        .expect("execution should be present");
                    let mut buyer = execution.buyer.expect("buyer should be present");
                    let mut seller = execution.seller.expect("seller should be present");
                    let buyer_key = buyer.key.expect("buyer key should be present");
                    let seller_key = seller.key.expect("seller key should be present");

                    if buyer.remaining == 0 {
                        buyer.set_status(OrderStatus::Filled);
                    }

                    if seller.remaining == 0 {
                        seller.set_status(OrderStatus::Filled);
                    }
                    updates.push((buyer_key, buyer));
                    updates.push((seller_key, seller));
                }
            }
        }
        self.inner.persister.save_batch(updates, vec![])?;
        Ok(())
    }

    fn get_order(&self, key: Key) -> Result<Option<Order>, E> {
        self.inner.persister.load(key)
    }
}

impl AllOrders<DB, DB, PersisterError<rocksdb::Error>> {
    pub fn init(
        path: &str,
        wal: WAL<DB, PersisterError<rocksdb::Error>>,
    ) -> Result<Self, PersisterError<rocksdb::Error>> {
        let mut options = Options::default();
        options.create_if_missing(true);
        let persister = DB::open(&options, path).expect("failed to open db");
        let all_orders = AllOrders {
            inner: Arc::new(Inner {
                persister,
                wal,
                alive: AtomicBool::new(true),
            }),
        };
        all_orders.sync_forever_thread();
        Ok(all_orders)
    }
}
