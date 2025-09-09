mod all_orders;
mod logs;
mod order_book;
mod order_book_persister;
mod persister;
mod protos;

use std::sync::atomic::{AtomicU32, Ordering};

use rocksdb::DB;

use crate::{
    all_orders::AllOrders,
    logs::WAL,
    order_book::OrderBook,
    order_book_persister::OrderBookPersister,
    persister::PersisterError,
    protos::{Key, Order, OrderStatus, OrderType, Side},
};

#[tokio::main]
async fn main() {
    let server = Server::new();
}

struct Server {
    wal: WAL<DB, PersisterError<rocksdb::Error>>,
    all_orders: AllOrders<DB, DB, PersisterError<rocksdb::Error>>,
    order_book_persister: OrderBookPersister<DB, DB, PersisterError<rocksdb::Error>>,
    order_book: OrderBook<WAL<DB, PersisterError<rocksdb::Error>>>,
}

impl Server {
    pub fn new() -> Self {
        let wal = WAL::init("data/wal").expect("failed to init wal");
        let all_orders =
            AllOrders::init("data/all_orders", wal.clone()).expect("failed to init all orders");
        let order_book_persister = OrderBookPersister::init("data/order_book", wal.clone())
            .expect("failed to init order book persister");
        let order_book = order_book_persister
            .load_order_book()
            .expect("failed to load order book");
        Self {
            wal,
            all_orders,
            order_book_persister,
            order_book,
        }
    }
}

fn next_sequence() -> u32 {
    static SEQUENCE: AtomicU32 = AtomicU32::new(0);
    SEQUENCE.fetch_add(1, Ordering::Relaxed)
}

fn new_order(
    price: f32,
    quantity: u64,
    side: Side,
    order_type: OrderType,
    status: OrderStatus,
    remaining: u64,
    timestamp: u64,
    sequence: u32,
) -> Order {
    Order {
        key: Some(Key {
            prefix: u32::from_le_bytes([0u8, 0u8, 0u8, 'o' as u8]),
            price,
            timestamp,
            sequence,
        }),
        side: side.into(),
        order_type: order_type.into(),
        quantity,
        remaining,
        status: status.into(),
    }
}

#[cfg(test)]
mod tests {
    use core::f32;
    use std::{
        sync::OnceLock,
        thread,
        time::{Duration, Instant, SystemTime},
    };

    use tokio::{
        spawn,
        sync::mpsc::{Receiver, Sender, channel},
    };

    use super::*;
    use quickcheck::{Arbitrary, Gen};
    use quickcheck_macros::quickcheck;
    use tokio::runtime::Runtime;

    fn gaussian(g: &mut Gen, mean: f32, std_dev: f32) -> f32 {
        let x = f32::arbitrary(g);
        let x = match x {
            -1.0..=1.0 => x,
            f32::INFINITY => 1.0,
            f32::NEG_INFINITY => -1.0,
            _ if x.is_nan() => 0.0,
            _ => x / f32::MAX,
        };
        let x = if x < 0.0 {
            -x.abs().sqrt()
        } else {
            x.abs().sqrt()
        };
        let res = mean + std_dev * x;
        if res == 0.0 { 1e-10 } else { res }
    }

    impl Arbitrary for Side {
        fn arbitrary(g: &mut Gen) -> Self {
            g.choose(&[Side::Buy, Side::Sell])
                .expect("should choose side")
                .clone()
        }
    }

    impl Arbitrary for OrderType {
        fn arbitrary(g: &mut Gen) -> Self {
            g.choose(&[
                OrderType::Limit,
                OrderType::Market,
                OrderType::Stop,
                OrderType::StopLimit,
            ])
            .expect("should choose order type")
            .clone()
        }
    }

    impl Arbitrary for OrderStatus {
        fn arbitrary(g: &mut Gen) -> Self {
            g.choose(&[
                OrderStatus::Open,
                OrderStatus::PartiallyFilled,
                OrderStatus::Filled,
                OrderStatus::Cancelled,
            ])
            .expect("should choose order status")
            .clone()
        }
    }

    impl Arbitrary for Key {
        fn arbitrary(g: &mut Gen) -> Self {
            Key {
                prefix: u32::from_le_bytes([0u8, 0u8, 0u8, 'o' as u8]),
                price: gaussian(g, 100.0, 100.0),
                timestamp: SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .expect("time should be after UNIX_EPOCH")
                    .as_nanos() as u64,
                sequence: u32::arbitrary(g),
            }
        }
    }

    impl Arbitrary for Order {
        fn arbitrary(g: &mut Gen) -> Self {
            let quantity = gaussian(g, 100.0, 99.0).ceil() as u64 + 1;
            Order {
                key: Some(Key::arbitrary(g)),
                side: Side::arbitrary(g).into(),
                order_type: OrderType::arbitrary(g).into(),
                quantity: quantity,
                remaining: quantity,
                status: OrderStatus::Open.into(),
            }
        }
    }

    static CHANNEL: OnceLock<Sender<Order>> = OnceLock::new();

    fn serve() -> Sender<Order> {
        let sender = CHANNEL.get_or_init(|| {
            let (sender, mut receiver) = channel(65535);
            thread::spawn(move || {
                Runtime::new()
                    .expect("failed to create runtime")
                    .block_on(async {
                        let mut server = Server::new();
                        println!("server started");
                        while let Some(order) = receiver.recv().await {
                            let fut = server.order_book.add_order(order);
                            spawn(async move {
                                if let Err(e) = fut.await {
                                    tracing::error!("failed to add order: {:?}", e);
                                }
                            });
                        }
                        println!("server stopped");
                    })
            });
            sender
        });
        sender.clone()
    }

    #[quickcheck]
    fn test_new_order(order: Order) {
        println!("sending order: {:?}", order);
        let sender = serve();
        if let Err(e) = sender.blocking_send(order) {
            tracing::error!("failed to send order: {:?}", e);
        }
    }

    #[quickcheck]
    fn test_new_orders(orders: Vec<Order>) {
        let sender = serve();
        for order in orders {
            println!("sending order: {:?}", order);
            if let Err(e) = sender.blocking_send(order) {
                tracing::error!("failed to send order: {:?}", e);
            }
        }
    }

    #[test]
    fn load_order_book() {
        let sender = serve();
        let mut g = Gen::new(1000);
        for _ in 0..10 {
            let orders = Vec::<Order>::arbitrary(&mut g);
            for order in orders {
                if let Err(e) = sender.blocking_send(order) {
                    tracing::error!("failed to send order: {:?}", e);
                }
            }
            thread::sleep(Duration::from_millis(10));
        }

        thread::sleep(Duration::from_secs(1));
    }

    #[test]
    fn test_load_order_book() {
        let server = Server::new();
        println!("order book: {:?}", server.order_book.buys.len());
        println!("order book: {:?}", server.order_book.sells.len());
        println!(
            "last sequence: {:?}",
            server.wal.fetch_logs_by_sequence(20000, 1024)
        );
    }
}
