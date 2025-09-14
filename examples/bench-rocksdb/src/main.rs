#![deny(warnings)]
#![deny(clippy::unwrap_used)]
#![allow(dead_code)]

use openmatching::{order_book::OrderBook, protos::Order};
use openmatching_persister_rocksdb::{AsyncRocksdbPersister, RocksdbPersister};

use std::time::Instant;

use tokio::{join, spawn, sync::mpsc::channel};

use quickcheck::{Arbitrary, Gen};


pub struct Server {
    pub order_book: OrderBook<AsyncRocksdbPersister>,
}

impl Server {
    pub async fn new() -> Self {
        let database = RocksdbPersister::new_async("data/order_book");
        let order_book = OrderBook::create(database).await.expect("failed to create order book");
        Self { order_book }
    }
}


#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let (sender, mut receiver) = channel(65535);
    // let (fut_tx, fut_rx) = std::sync::mpsc::sync_channel(65535);
    let (fut_tx, mut fut_rx) = channel(65535);
    let spawned_handle = spawn(async move {
        let mut join_set = tokio::task::JoinSet::new();
        while let Some(futs) = fut_rx.recv().await {
            for fut in futs {
                join_set.spawn(fut);
            }
        }
        join_set.join_all().await;
    });

    let order_gen_handle = spawn(async move {
        let mut g = Gen::new(1000);
        let mut count = 0;
        let now = Instant::now();
        for _ in 0..10000 {
            let orders = Vec::<Order>::arbitrary(&mut g);
            count += orders.len();
            if let Err(e) = sender.send(orders).await {
                tracing::error!("failed to send order: {e:?}");
            }
        }
        tracing::info!("time: {:?}, count: {:?}", now.elapsed(), count);
    });

    let order_book_handle = spawn(async move {
        let mut server = Server::new().await;
        tracing::info!("server started");
        while let Some(orders) = receiver.recv().await {
            let mut futs = vec![];
            for order in orders {
                let fut = server.order_book.add_order(order);
                futs.push(Box::pin(fut));
            }
            let _ = fut_tx.send(futs).await;
        }
    });
    let (res1, res2, res3) = join!(order_gen_handle, spawned_handle, order_book_handle);
    res1.expect("failed to join order gen handle");
    res2.expect("failed to join spawned handle");
    res3.expect("failed to join order book handle");
    tracing::info!("server stopped");
}
