use std::{
    error::Error,
    fmt::Debug,
    sync::{Arc, Mutex},
    thread::{self, JoinHandle},
};

use tokio::sync::{mpsc, oneshot};

use crate::protos::{Order, TimebasedKey};

pub trait Persister: Send + Sync + 'static {
    type Error: Error + Debug + Send + Sync + 'static;

    fn upsert_order(&self, orders: Vec<Order>) -> Result<(), Self::Error>;
    fn get_order(&self, key: TimebasedKey) -> Result<Option<Order>, Self::Error>;
    fn get_all_buy_orders(&self) -> Result<Vec<Order>, Self::Error>;
    fn get_all_sell_orders(&self) -> Result<Vec<Order>, Self::Error>;
}

pub trait AsyncPersister {
    type Error: Error + Debug + Send + Sync + 'static;

    fn upsert_order(
        &self,
        orders: Vec<Order>,
    ) -> impl Future<Output = Result<(), Self::Error>> + 'static;
    fn get_order(
        &self,
        key: TimebasedKey,
    ) -> impl Future<Output = Result<Option<Order>, Self::Error>> + 'static;
    fn get_all_buy_orders(&self)
    -> impl Future<Output = Result<Vec<Order>, Self::Error>> + 'static;
    fn get_all_sell_orders(
        &self,
    ) -> impl Future<Output = Result<Vec<Order>, Self::Error>> + 'static;
}

enum Command<T: Persister + 'static> {
    Upsert {
        reply: oneshot::Sender<Result<(), T::Error>>,
        orders: Vec<Order>,
    },
    Get {
        reply: oneshot::Sender<Result<Option<Order>, T::Error>>,
        key: TimebasedKey,
    },
    GetAllBuyOrders {
        reply: oneshot::Sender<Result<Vec<Order>, T::Error>>,
    },
    GetAllSellOrders {
        reply: oneshot::Sender<Result<Vec<Order>, T::Error>>,
    },
    Close,
}

#[derive(Clone)]
pub struct Asyncify<T: Persister + Send + Sync + 'static> {
    tx: mpsc::Sender<Command<T>>,
    handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl<T: Persister + Send + Sync + 'static> Asyncify<T> {
    pub fn new(persister: T) -> Self {
        let (tx, mut rx) = mpsc::channel(65535);
        let handle = thread::spawn(move || {
            while let Some(command) = rx.blocking_recv() {
                match command {
                    Command::Close => break,
                    Command::Upsert { reply, orders } => {
                        let res = persister.upsert_order(orders);
                        if reply.send(res).is_err() {
                            tracing::warn!("failed to send upsert reply");
                        }
                    }
                    Command::Get { reply, key } => {
                        let res = persister.get_order(key);
                        if reply.send(res).is_err() {
                            tracing::warn!("failed to send get order reply");
                        }
                    }
                    Command::GetAllBuyOrders { reply } => {
                        let res = persister.get_all_buy_orders();
                        if reply.send(res).is_err() {
                            tracing::warn!("failed to send get all buy orders reply");
                        }
                    }
                    Command::GetAllSellOrders { reply } => {
                        let res = persister.get_all_sell_orders();
                        if reply.send(res).is_err() {
                            tracing::warn!("failed to send get all sell orders reply");
                        }
                    }
                }
            }
        });

        Self {
            tx,
            handle: Arc::new(Mutex::new(Some(handle))),
        }
    }
}

impl<T: Persister + Send + Sync + 'static> Drop for Asyncify<T> {
    fn drop(&mut self) {
        while let Err(e) = self.tx.try_send(Command::Close) {
            tracing::warn!("failed to send command: {:?}", e);
        }

        self.handle
            .lock()
            .expect("failed to lock handle")
            .take()
            .expect("handle should be present")
            .join()
            .expect("failed to join handle");
    }
}

impl<T: Persister + 'static> AsyncPersister for Asyncify<T> {
    type Error = T::Error;

    fn upsert_order(
        &self,
        orders: Vec<Order>,
    ) -> impl Future<Output = Result<(), Self::Error>> + 'static {
        let tx = self.tx.clone();
        async move {
            let (reply, rx) = oneshot::channel();
            tx.send(Command::Upsert { reply, orders })
                .await
                .expect("failed to send command");
            rx.await.expect("failed to receive reply")
        }
    }

    fn get_order(
        &self,
        key: TimebasedKey,
    ) -> impl Future<Output = Result<Option<Order>, Self::Error>> + 'static {
        let tx = self.tx.clone();
        async move {
            let (reply, rx) = oneshot::channel();
            tx.send(Command::Get { reply, key })
                .await
                .expect("failed to send command");
            rx.await.expect("failed to receive reply")
        }
    }

    fn get_all_buy_orders(
        &self,
    ) -> impl Future<Output = Result<Vec<Order>, Self::Error>> + 'static {
        let tx = self.tx.clone();
        async move {
            let (reply, rx) = oneshot::channel();
            tx.send(Command::GetAllBuyOrders { reply })
                .await
                .expect("failed to send command");
            rx.await.expect("failed to receive reply")
        }
    }

    fn get_all_sell_orders(
        &self,
    ) -> impl Future<Output = Result<Vec<Order>, Self::Error>> + 'static {
        let tx = self.tx.clone();
        async move {
            let (reply, rx) = oneshot::channel();
            tx.send(Command::GetAllSellOrders { reply })
                .await
                .expect("failed to send command");
            rx.await.expect("failed to receive reply")
        }
    }
}
