use std::{
    error::Error,
    sync::{Arc, Mutex},
    thread::{self, JoinHandle},
};

use rocksdb::{ColumnFamilyRef, DB, IteratorMode, Options, ReadOptions, WriteBatch};
use tokio::sync::{mpsc, oneshot};

use crate::protos::{ToBytes, TryFromBytes};

pub trait Persister<V>
where
    V: TryFromBytes + ToBytes,
{
    type Error: Error;
    type Iter<'a>: Iterator<Item = Result<([u8; 16], V), Self::Error>> + 'a
    where
        Self: 'a,
        V: 'a;

    fn save(
        &self,
        updates: Vec<(&'static str, [u8; 16], V)>,
        deletes: Vec<(&'static str, [u8; 16])>,
    ) -> Result<(), Self::Error>;

    fn load(&self, cf: &'static str, key: [u8; 16]) -> Result<Option<V>, Self::Error>;

    fn load_prefix_iter<'a>(
        &'a self,
        cf: &'static str,
        prefix: &[u8],
    ) -> Result<Self::Iter<'a>, Self::Error>
    where
        V: 'a;

    #[allow(dead_code)]
    fn load_range_iter<'a>(
        &'a self,
        cf: &'static str,
        start: [u8; 16],
        end: [u8; 16],
    ) -> Result<Self::Iter<'a>, Self::Error>
    where
        V: 'a;

    fn load_all_iter<'a>(&'a self, cf: &'static str) -> Result<Self::Iter<'a>, Self::Error>
    where
        V: 'a;
}

#[derive(thiserror::Error, Debug)]
pub enum PersisterError<T: Error> {
    #[error("db error")]
    DB(#[from] T),
    #[error("failed to decode value")]
    DecodeValue(Box<dyn Error + Send + Sync + 'static>),
}

#[allow(dead_code)]
const META_CF: &str = "meta";
#[allow(dead_code)]
const LAST_SEQUENCE_KEY: &str = "last_sequence";

trait ColumnFamily {
    fn cf(&self, name: &'static str) -> ColumnFamilyRef;
}

impl ColumnFamily for DB {
    fn cf(&self, name: &'static str) -> ColumnFamilyRef {
        if let Some(cf) = self.cf_handle(name) {
            cf
        } else {
            panic!("failed to get cf handle: {name}");
        }
    }
}

impl<V> Persister<V> for DB
where
    V: TryFromBytes + ToBytes,
{
    type Error = PersisterError<rocksdb::Error>;

    type Iter<'a>
        = Box<dyn Iterator<Item = Result<([u8; 16], V), Self::Error>> + 'a>
    where
        Self: 'a,
        V: 'a;

    fn load(&self, cf: &'static str, key: [u8; 16]) -> Result<Option<V>, Self::Error> {
        let Some(value) = self.get_cf(&self.cf(cf), key)? else {
            return Ok(None);
        };

        let value =
            V::try_from_bytes(&value[..]).map_err(|e| Self::Error::DecodeValue(Box::new(e)))?;
        Ok(Some(value))
    }

    fn load_prefix_iter<'a>(
        &'a self,
        cf: &'static str,
        prefix: &[u8],
    ) -> Result<Self::Iter<'a>, Self::Error>
    where
        V: 'a,
    {
        let iter = self.prefix_iterator_cf(&self.cf(cf), prefix).map(|result| {
            let (raw_key, value) = result?;
            let mut key: [u8; 16] = [0; 16];
            key.copy_from_slice(&raw_key[..]);
            let value =
                V::try_from_bytes(&value[..]).map_err(|e| Self::Error::DecodeValue(Box::new(e)))?;
            Ok((key, value))
        });
        Ok(Box::new(iter))
    }

    fn load_range_iter<'a>(
        &'a self,
        cf: &'static str,
        start: [u8; 16],
        end: [u8; 16],
    ) -> Result<Self::Iter<'a>, Self::Error>
    where
        V: 'a,
    {
        let mut readopts = ReadOptions::default();
        readopts.set_iterate_upper_bound(end);
        readopts.set_iterate_lower_bound(start);
        let iter = self
            .iterator_cf_opt(&self.cf(cf), readopts, IteratorMode::Start)
            .map(|result| {
                let (raw_key, value) = result?;
                let mut key: [u8; 16] = [0; 16];
                key.copy_from_slice(&raw_key[..]);
                let value = V::try_from_bytes(&value[..])
                    .map_err(|e| Self::Error::DecodeValue(Box::new(e)))?;
                Ok((key, value))
            });
        Ok(Box::new(iter))
    }

    fn load_all_iter<'a>(&'a self, cf: &'static str) -> Result<Self::Iter<'a>, Self::Error>
    where
        V: 'a,
    {
        let iter = self
            .full_iterator_cf(&self.cf(cf), IteratorMode::Start)
            .map(|result| {
                let (raw_key, value) = result?;
                let mut key: [u8; 16] = [0; 16];
                key.copy_from_slice(&raw_key[..]);
                let value = V::try_from_bytes(&value[..])
                    .map_err(|e| Self::Error::DecodeValue(Box::new(e)))?;
                Ok((key, value))
            });

        Ok(Box::new(iter))
    }

    fn save(
        &self,
        updates: Vec<(&'static str, [u8; 16], V)>,
        deletes: Vec<(&'static str, [u8; 16])>,
    ) -> Result<(), Self::Error> {
        let mut write_batch = WriteBatch::new();
        for (cf, key, value) in updates {
            write_batch.put_cf(&self.cf(cf), key, value.to_bytes());
        }
        for (cf, key) in deletes {
            write_batch.delete_cf(&self.cf(cf), key);
        }
        self.write(write_batch)?;
        Ok(())
    }
}

pub trait AsyncPersister<V>
where
    V: TryFromBytes + ToBytes,
{
    type Error: Error;
    type Iter<'a>: Iterator<Item = Result<([u8; 16], V), Self::Error>> + 'a
    where
        Self: 'a,
        V: 'a;

    fn save(
        &self,
        updates: Vec<(&'static str, [u8; 16], V)>,
        deletes: Vec<(&'static str, [u8; 16])>,
    ) -> impl Future<Output = Result<(), Self::Error>> + 'static;

    fn load(
        &self,
        cf: &'static str,
        key: [u8; 16],
    ) -> impl Future<Output = Result<Option<V>, Self::Error>> + 'static;

    #[allow(dead_code)]
    fn load_prefix_iter<'a>(
        &'a self,
        cf: &'static str,
        prefix: &[u8],
    ) -> Result<Self::Iter<'a>, Self::Error>
    where
        V: 'a;

    fn load_all_iter<'a>(&'a self, cf: &'static str) -> Result<Self::Iter<'a>, Self::Error>
    where
        V: 'a;
}

enum Command<K, V, E> {
    Save {
        reply: oneshot::Sender<Result<(), E>>,
        updates: Vec<(&'static str, K, V)>,
        deletes: Vec<(&'static str, K)>,
    },
    Load {
        reply: oneshot::Sender<Result<Option<V>, E>>,
        cf: &'static str,
        key: K,
    },
    Close,
}

struct Inner<V> {
    db: DB,
    tx: mpsc::Sender<Command<[u8; 16], V, PersisterError<rocksdb::Error>>>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

impl<V> Drop for Inner<V> {
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

#[derive(Clone)]
pub struct Database<V> {
    inner: Arc<Inner<V>>,
}

impl<V> AsyncPersister<V> for Database<V>
where
    V: TryFromBytes + ToBytes + 'static,
{
    type Error = PersisterError<rocksdb::Error>;
    type Iter<'a>
        = Box<dyn Iterator<Item = Result<([u8; 16], V), Self::Error>> + 'a>
    where
        Self: 'a,
        V: 'a;

    fn save(
        &self,
        updates: Vec<(&'static str, [u8; 16], V)>,
        deletes: Vec<(&'static str, [u8; 16])>,
    ) -> impl Future<Output = Result<(), Self::Error>> + 'static {
        let tx = self.inner.tx.clone();
        async move {
            let (reply, rx) = oneshot::channel();
            tx.send(Command::Save {
                reply,
                updates,
                deletes,
            })
            .await
            .expect("failed to send command");
            rx.await.expect("failed to receive reply")
        }
    }

    fn load(
        &self,
        cf: &'static str,
        key: [u8; 16],
    ) -> impl Future<Output = Result<Option<V>, Self::Error>> + 'static {
        let tx = self.inner.tx.clone();
        async move {
            let (reply, rx) = oneshot::channel();
            tx.send(Command::Load { reply, cf, key })
                .await
                .expect("failed to send command");
            rx.await.expect("failed to receive reply")
        }
    }

    fn load_prefix_iter<'a>(
        &'a self,
        cf: &'static str,
        prefix: &[u8],
    ) -> Result<Self::Iter<'a>, Self::Error>
    where
        V: 'a,
    {
        self.inner.db.load_prefix_iter(cf, prefix)
    }

    fn load_all_iter<'a>(&'a self, cf: &'static str) -> Result<Self::Iter<'a>, Self::Error>
    where
        V: 'a,
    {
        self.inner.db.load_all_iter(cf)
    }
}

impl<V> Database<V>
where
    V: TryFromBytes + ToBytes + 'static,
{
    #[allow(dead_code)]
    pub fn new(path: &'static str) -> Result<Self, PersisterError<rocksdb::Error>> {
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
        let (tx, mut rx) = mpsc::channel(u16::MAX as usize);

        let placeholder = Mutex::new(None);
        let inner = Arc::new(Inner {
            db,
            tx,
            handle: placeholder,
        });
        let bind_inner = Arc::downgrade(&inner);
        let handle = thread::spawn(move || {
            let mut start = None;
            let mut count: usize = 0;
            while let Some(command) = rx.blocking_recv() {
                if start.is_none() {
                    start = Some(std::time::Instant::now());
                }
                match command {
                    Command::Close => break,
                    Command::Save {
                        reply,
                        updates,
                        deletes,
                    } => {
                        let bind_inner = bind_inner.upgrade().expect("inner should be present");
                        let deletes_len = deletes.len();
                        let res = bind_inner.db.save(updates, deletes);
                        if let Err(e) = reply.send(res) {
                            tracing::warn!("failed to send reply: {:?}", e);
                        } else {
                            count += deletes_len;
                        }
                    }
                    Command::Load { reply, cf, key } => {
                        let bind_inner = bind_inner.upgrade().expect("inner should be present");
                        let value = bind_inner.db.load(cf, key);
                        if reply.send(value).is_err() {
                            tracing::warn!("failed to send reply");
                        }
                    }
                }
            }
            let elapsed = start.map(|s| s.elapsed()).unwrap_or_default();
            tracing::info!(
                "persister thread stopped, count: {:?}, time: {:?}, {:?} tps",
                count,
                elapsed,
                count as f64 / elapsed.as_secs_f64()
            );
        });
        *inner.handle.lock().expect("failed to lock handle") = Some(handle);

        Ok(Self { inner })
    }
}

#[cfg(test)]
mod tests {
    use rocksdb::Options;

    use super::*;

    #[test]
    fn test_metadata_store() {
        let mut options = Options::default();
        options.create_if_missing(true);
        let _ = DB::open(&options, "data/test.db").unwrap();
    }
}
