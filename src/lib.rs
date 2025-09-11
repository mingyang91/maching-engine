pub mod order_book;
pub mod persister;
pub mod protos;

use crate::{order_book::OrderBook, persister::Database, protos::Order};

pub struct Server {
    pub order_book: OrderBook<Database<Order>>,
}

impl Server {
    pub fn new() -> Self {
        let database = Database::new("data/order_book").expect("failed to create database");
        let order_book = OrderBook::create(database).expect("failed to create order book");
        Self { order_book }
    }
}
