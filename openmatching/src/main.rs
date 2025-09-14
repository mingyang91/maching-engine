#![deny(warnings)]
#![deny(clippy::unwrap_used)]

mod borrow;
mod order_book;
mod persister;
mod protos;

use openmatching::Server;

#[tokio::main]
async fn main() {
    let server = Server::new();
    drop(server);
}
