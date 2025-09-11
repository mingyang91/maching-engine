#![deny(warnings)]
#![deny(clippy::unwrap_used)]

mod order_book;
mod persister;
mod protos;

use matching_engine::Server;

#[tokio::main]
async fn main() {
    let server = Server::new();
    drop(server);
}
