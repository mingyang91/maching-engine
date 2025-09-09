mod order_book;
mod persister;
mod protos;

use crate::{
    order_book::OrderBook,
    persister::Database,
    protos::{FixedKey, Order},
};

#[tokio::main]
async fn main() {
    let server = Server::new();
}

struct Server {
    order_book: OrderBook<Database<FixedKey, Order>>,
}

impl Server {
    pub fn new() -> Self {
        let database = Database::new("data/order_book").expect("failed to create database");
        let order_book = OrderBook::create(database).expect("failed to create order book");
        Self { order_book }
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

    use prost::Message;
    use tokio::{
        spawn,
        sync::mpsc::{Sender, channel},
    };

    use crate::protos::{Key, Order, OrderStatus, OrderType, Side};

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
        let mut g = Gen::new(10000);
        let mut count = 0;
        let now = Instant::now();
        for _ in 0..10000 {
            let orders = Vec::<Order>::arbitrary(&mut g);
            count += orders.len();
            for order in orders {
                if let Err(e) = sender.blocking_send(order) {
                    tracing::error!("failed to send order: {:?}", e);
                }
            }
        }
        println!("time: {:?}, count: {:?}", now.elapsed(), count);
    }

    #[quickcheck]
    fn add_order(order: Order) {
        let sender = serve();
        if let Err(e) = sender.blocking_send(order) {
            println!("failed to send order: {:?}", e);
        }
        println!("added order: {:?}", order);
    }

    #[test]
    fn test_load_order_book() {
        let server = Server::new();

        println!("order book: {:?}", server.order_book.buys.len());
        println!("order book: {:?}", server.order_book.sells.len());
    }

    #[quickcheck]
    fn enc_dec_key(key: Key) {
        let encoded = key.encode_to_vec();
        println!("encoded: {:?}", hex::encode(&encoded[..]));
        let decoded = Key::decode(&encoded[..]).expect("failed to decode key");
        assert_eq!(key, decoded);
    }
}
