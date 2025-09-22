use std::sync::Arc;

use openmatching::{
    persister::{Asyncify, Persister},
    protos::{Order, OrderStatus, Side, TimebasedKey},
};
use prost::Message;
use rocksdb::{DB, IteratorMode, Options, WriteBatch};

#[derive(thiserror::Error, Debug)]
pub enum RocksdbPersisterError {
    #[error("db error")]
    DB(#[from] rocksdb::Error),
    #[error("failed to decode value")]
    DecodeValue(#[from] prost::DecodeError),
}

struct Inner {
    db: DB,
}

pub type AsyncRocksdbPersister = Asyncify<RocksdbPersister>;

#[derive(Clone)]
pub struct RocksdbPersister {
    inner: Arc<Inner>,
}

impl RocksdbPersister {
    pub fn new(path: &str) -> Self {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);

        // Open with ALL column families - don't be an amateur
        let cfs = vec!["default", "buys", "sells", "all_orders", "meta"];
        let db = match DB::list_cf(&options, path) {
            Ok(existing_cfs) => {
                // Database exists, open with existing column families
                DB::open_cf(&options, path, existing_cfs).expect("failed to open db with cfs")
            }
            Err(_) => {
                // Fresh database, create with our column families
                DB::open_cf(&options, path, &cfs).expect("failed to create db with cfs")
            }
        };

        let inner = Inner { db };
        RocksdbPersister {
            inner: Arc::new(inner),
        }
    }

    pub fn new_async(path: &str) -> AsyncRocksdbPersister {
        Asyncify::new(RocksdbPersister::new(path))
    }

    fn all_orders_cf(&self) -> rocksdb::ColumnFamilyRef<'_> {
        self.inner
            .db
            .cf_handle("all_orders")
            .expect("failed to get cf handle")
    }

    fn buys_cf(&self) -> rocksdb::ColumnFamilyRef<'_> {
        self.inner
            .db
            .cf_handle("buys")
            .expect("failed to get cf handle")
    }

    fn sells_cf(&self) -> rocksdb::ColumnFamilyRef<'_> {
        self.inner
            .db
            .cf_handle("sells")
            .expect("failed to get cf handle")
    }

    #[allow(dead_code)]
    fn meta_cf(&self) -> rocksdb::ColumnFamilyRef<'_> {
        self.inner
            .db
            .cf_handle("meta")
            .expect("failed to get cf handle")
    }
}

impl Persister for RocksdbPersister {
    type Error = RocksdbPersisterError;

    fn upsert_order(&self, orders: Vec<Order>) -> Result<(), Self::Error> {
        let mut write_batch = WriteBatch::new();
        let all_orders_cf = self.all_orders_cf();
        let buys_cf = self.buys_cf();
        let sells_cf = self.sells_cf();
        for order in orders {
            let key: TimebasedKey = order.key.expect("key should be present").into();
            write_batch.put_cf(
                &all_orders_cf,
                key.to_pricebased().to_bytes(),
                order.encode_to_vec(),
            );
            if order.status() == OrderStatus::Cancelled || order.status() == OrderStatus::Filled {
                write_batch.delete_cf(&all_orders_cf, key.to_pricebased().to_bytes());
            } else {
                let cf = if order.side() == Side::Buy {
                    &buys_cf
                } else {
                    &sells_cf
                };
                write_batch.put_cf(cf, key.to_pricebased().to_bytes(), order.encode_to_vec());
            }
        }
        self.inner.db.write(write_batch)?;
        Ok(())
    }

    fn get_order(
        &self,
        _side: Side,
        key: openmatching::protos::TimebasedKey,
    ) -> Result<Option<openmatching::protos::Order>, Self::Error> {
        let cf = self.all_orders_cf();
        let value = self.inner.db.get_cf(&cf, key.to_pricebased().to_bytes())?;
        let Some(value) = value else {
            return Ok(None);
        };

        let order = openmatching::protos::Order::decode(value.as_slice())?;
        Ok(Some(order))
    }

    fn get_all_buy_orders(&self) -> Result<Vec<openmatching::protos::Order>, Self::Error> {
        let cf = self.buys_cf();
        let iter = self.inner.db.iterator_cf(&cf, IteratorMode::Start);
        let mut orders = Vec::new();
        for result in iter {
            let (_, value) = result?;
            orders.push(openmatching::protos::Order::decode(&value[..])?);
        }
        Ok(orders)
    }

    fn get_all_sell_orders(&self) -> Result<Vec<openmatching::protos::Order>, Self::Error> {
        let cf = self.sells_cf();
        let iter = self.inner.db.iterator_cf(&cf, IteratorMode::Start);
        let mut orders = Vec::new();
        for result in iter {
            let (_, value) = result?;
            orders.push(openmatching::protos::Order::decode(&value[..])?);
        }
        Ok(orders)
    }
}
