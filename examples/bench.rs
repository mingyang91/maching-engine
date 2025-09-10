#![deny(warnings)]
#![deny(clippy::unwrap_used)]
#![allow(dead_code)]

use matching_engine::{Server, protos::Order};

use std::time::Instant;

use tokio::{join, spawn, sync::mpsc::channel};

use quickcheck::{Arbitrary, Gen};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let (sender, mut receiver) = channel(65535);
    let (fut_tx, fut_rx) = std::sync::mpsc::sync_channel(65535);
    let spawned_handle = spawn(async move {
        while let Ok(futs) = fut_rx.recv() {
            for fut in futs {
                spawn(fut);
            }
        }
    });

    let order_gen_handle = spawn(async move {
        let mut g = Gen::new(10000);
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
        let mut server = Server::new();
        tracing::info!("server started");
        while let Some(orders) = receiver.recv().await {
            let mut futs = vec![];
            for order in orders {
                let fut = server.order_book.add_order(order);
                futs.push(Box::pin(fut));
            }
            let _ = fut_tx.send(futs);
        }
    });
    let _ = join!(order_gen_handle, spawned_handle, order_book_handle);
    tracing::info!("server stopped");
}
