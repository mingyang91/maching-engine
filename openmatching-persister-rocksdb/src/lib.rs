use std::{ptr::NonNull, sync::Arc};

use openmatching::{
    persister::{Asyncify, Persister},
    protos::{Order, OrderStatus, Side, TimebasedKey},
};
use prost::Message;
use rocksdb::{ColumnFamilyRef, DB, Options, WriteBatch};

#[derive(thiserror::Error, Debug)]
pub enum RocksdbPersisterError {
    #[error("db error")]
    DB(#[from] rocksdb::Error),
    #[error("failed to decode value")]
    DecodeValue(#[from] prost::DecodeError),
}

struct Inner<'a> {
    db: DB,
    all_orders_cf: NonNull<ColumnFamilyRef<'a>>,
    buys_cf: NonNull<ColumnFamilyRef<'a>>,
    sells_cf: NonNull<ColumnFamilyRef<'a>>,
    meta_cf: NonNull<ColumnFamilyRef<'a>>,
}

unsafe impl<'a> Send for Inner<'a> {}

unsafe impl<'a> Sync for Inner<'a> {}

pub type AsyncRocksdbPersister = Asyncify<RocksdbPersister>;

#[derive(Clone)]
pub struct RocksdbPersister {
    inner: Arc<Inner<'static>>,
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

        let inner = Inner {
            db,
            all_orders_cf: NonNull::dangling(),
            buys_cf: NonNull::dangling(),
            sells_cf: NonNull::dangling(),
            meta_cf: NonNull::dangling(),
        };

        unsafe {
            std::mem::forget(
                inner.all_orders_cf.replace(
                    inner
                        .db
                        .cf_handle("all_orders")
                        .expect("failed to get cf handle"),
                ),
            );
            std::mem::forget(
                inner
                    .buys_cf
                    .replace(inner.db.cf_handle("buys").expect("failed to get cf handle")),
            );
            std::mem::forget(
                inner.sells_cf.replace(
                    inner
                        .db
                        .cf_handle("sells")
                        .expect("failed to get cf handle"),
                ),
            );
            std::mem::forget(
                inner
                    .meta_cf
                    .replace(inner.db.cf_handle("meta").expect("failed to get cf handle")),
            );
        }
        RocksdbPersister {
            inner: Arc::new(inner),
        }
    }

    pub fn new_async(path: &str) -> AsyncRocksdbPersister {
        Asyncify::new(RocksdbPersister::new(path))
    }

    pub fn all_orders_cf(&self) -> &ColumnFamilyRef {
        unsafe { self.inner.all_orders_cf.as_ref() }
    }

    pub fn buys_cf(&self) -> &ColumnFamilyRef {
        unsafe { self.inner.buys_cf.as_ref() }
    }

    pub fn sells_cf(&self) -> &ColumnFamilyRef {
        unsafe { self.inner.sells_cf.as_ref() }
    }

    pub fn meta_cf(&self) -> &ColumnFamilyRef {
        unsafe { self.inner.meta_cf.as_ref() }
    }
}

impl Persister for RocksdbPersister {
    type Error = RocksdbPersisterError;

    fn upsert_order(&self, orders: Vec<Order>) -> Result<(), Self::Error> {
        let mut write_batch = WriteBatch::new();
        for order in orders {
            let key: TimebasedKey = order.key.expect("key should be present").into();
            write_batch.put_cf(
                self.all_orders_cf(),
                key.to_pricebased().to_bytes(),
                order.encode_to_vec(),
            );
            if order.status() == OrderStatus::Cancelled || order.status() == OrderStatus::Filled {
                write_batch.delete_cf(self.all_orders_cf(), key.to_pricebased().to_bytes());
            } else {
                let cf = if order.side() == Side::Buy {
                    self.buys_cf()
                } else {
                    self.sells_cf()
                };
                write_batch.put_cf(cf, key.to_pricebased().to_bytes(), order.encode_to_vec());
            }
        }
        self.inner.db.write(write_batch)?;
        Ok(())
    }

    fn get_order(
        &self,
        key: openmatching::protos::TimebasedKey,
    ) -> Result<Option<openmatching::protos::Order>, Self::Error> {
        let value = self
            .inner
            .db
            .get_cf(self.all_orders_cf(), key.to_pricebased().to_bytes())?;
        if let Some(value) = value {
            Ok(Some(openmatching::protos::Order::decode(value.as_slice())?))
        } else {
            Ok(None)
        }
    }

    fn get_all_buy_orders(&self) -> Result<Vec<openmatching::protos::Order>, Self::Error> {
        let iter = self
            .inner
            .db
            .iterator_cf(self.buys_cf(), rocksdb::IteratorMode::Start);
        let mut orders = Vec::new();
        for result in iter {
            let (_, value) = result?;
            orders.push(openmatching::protos::Order::decode(&value[..])?);
        }
        Ok(orders)
    }

    fn get_all_sell_orders(&self) -> Result<Vec<openmatching::protos::Order>, Self::Error> {
        let iter = self
            .inner
            .db
            .iterator_cf(self.sells_cf(), rocksdb::IteratorMode::Start);
        let mut orders = Vec::new();
        for result in iter {
            let (_, value) = result?;
            orders.push(openmatching::protos::Order::decode(&value[..])?);
        }
        Ok(orders)
    }
}
