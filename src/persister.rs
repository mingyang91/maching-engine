use std::{
    error::Error,
    sync::{Arc, Mutex},
    thread::{self, JoinHandle},
};

use prost::Message;
use rocksdb::{ColumnFamilyRef, DB, IteratorMode, Options, ReadOptions, WriteBatch};
use tokio::sync::{mpsc, oneshot};

pub trait Persister<K, V>
where
    K: Message + Default,
    V: Message + Default,
{
    type Error: Error;
    type Iter<'a>: Iterator<Item = Result<(K, V), Self::Error>> + 'a
    where
        Self: 'a,
        K: 'a,
        V: 'a;

    fn last_sequence(&self) -> Result<u64, Self::Error>;
    fn put_last_sequence(&self, sequence: u64) -> Result<(), Self::Error>;

    fn save(
        &self,
        updates: Vec<(&'static str, K, V)>,
        deletes: Vec<(&'static str, K)>,
    ) -> Result<(), Self::Error>;

    fn load(&self, cf: &'static str, key: K) -> Result<Option<V>, Self::Error>;

    fn load_prefix_iter<'a>(
        &'a self,
        cf: &'static str,
        prefix: &[u8],
    ) -> Result<Self::Iter<'a>, Self::Error>
    where
        K: 'a,
        V: 'a;

    fn load_range_iter<'a>(
        &'a self,
        cf: &'static str,
        start: K,
        end: K,
    ) -> Result<Self::Iter<'a>, Self::Error>
    where
        K: 'a,
        V: 'a;

    fn load_all_iter<'a>(&'a self, cf: &'static str) -> Result<Self::Iter<'a>, Self::Error>
    where
        K: 'a,
        V: 'a;
}

#[derive(thiserror::Error, Debug)]
pub enum PersisterError<T: Error> {
    #[error("db error")]
    DBError(#[from] T),
    #[error("failed to decode key")]
    DecodeKeyError(prost::DecodeError),
    #[error("failed to decode value")]
    DecodeValueError(prost::DecodeError),
    #[error("failed to decode sequence")]
    DecodeSequenceError(prost::DecodeError),
}

const META_CF: &str = "meta";
const LAST_SEQUENCE_KEY: &str = "last_sequence";

trait ColumnFamily {
    fn cf(&self, name: &'static str) -> ColumnFamilyRef;
}

impl ColumnFamily for DB {
    fn cf(&self, name: &'static str) -> ColumnFamilyRef {
        self.cf_handle(name).expect("failed to get cf handle")
    }
}

impl<K, V> Persister<K, V> for DB
where
    K: Message + Default,
    V: Message + Default,
{
    type Error = PersisterError<rocksdb::Error>;

    type Iter<'a>
        = Box<dyn Iterator<Item = Result<(K, V), Self::Error>> + 'a>
    where
        Self: 'a,
        K: 'a,
        V: 'a;

    fn last_sequence(&self) -> Result<u64, Self::Error> {
        let Some(value) = self.get_cf(&self.cf(META_CF), LAST_SEQUENCE_KEY)? else {
            return Ok(0);
        };
        u64::decode(&value[..]).map_err(PersisterError::DecodeSequenceError)
    }

    fn put_last_sequence(&self, sequence: u64) -> Result<(), Self::Error> {
        self.put_cf(
            &self.cf(META_CF),
            LAST_SEQUENCE_KEY,
            &sequence.encode_to_vec(),
        )?;
        Ok(())
    }

    fn load(&self, cf: &'static str, key: K) -> Result<Option<V>, Self::Error> {
        let Some(value) = self.get_cf(&self.cf(cf), key.encode_to_vec())? else {
            return Ok(None);
        };

        let value = V::decode(&value[..]).map_err(Self::Error::DecodeValueError)?;
        Ok(Some(value))
    }

    fn load_prefix_iter<'a>(
        &'a self,
        cf: &'static str,
        prefix: &[u8],
    ) -> Result<Self::Iter<'a>, Self::Error>
    where
        K: 'a,
        V: 'a,
    {
        let iter = self.prefix_iterator_cf(&self.cf(cf), prefix).map(|result| {
            let (key, value) = result?;
            let key = K::decode(&key[..]).map_err(Self::Error::DecodeKeyError)?;
            let value = V::decode(&value[..]).map_err(Self::Error::DecodeValueError)?;
            Ok((key, value))
        });
        Ok(Box::new(iter))
    }

    fn load_range_iter<'a>(
        &'a self,
        cf: &'static str,
        start: K,
        end: K,
    ) -> Result<Self::Iter<'a>, Self::Error>
    where
        K: 'a,
        V: 'a,
    {
        let mut readopts = ReadOptions::default();
        readopts.set_iterate_upper_bound(end.encode_to_vec());
        readopts.set_iterate_lower_bound(start.encode_to_vec());
        let iter = self
            .iterator_cf_opt(&self.cf(cf), readopts, IteratorMode::Start)
            .map(|result| {
                let (key, value) = result?;
                let key = K::decode(&key[..]).map_err(Self::Error::DecodeKeyError)?;
                let value = V::decode(&value[..]).map_err(Self::Error::DecodeValueError)?;
                Ok((key, value))
            });
        Ok(Box::new(iter))
    }

    fn load_all_iter<'a>(&'a self, cf: &'static str) -> Result<Self::Iter<'a>, Self::Error>
    where
        K: 'a,
        V: 'a,
    {
        let iter = self
            .iterator_cf(&self.cf(cf), IteratorMode::Start)
            .map(|result| {
                let (key, value) = result?;
                let key = K::decode(&key[..]).map_err(Self::Error::DecodeKeyError)?;
                let value = V::decode(&value[..]).map_err(Self::Error::DecodeValueError)?;
                Ok((key, value))
            });
        Ok(Box::new(iter))
    }

    fn save(
        &self,
        updates: Vec<(&'static str, K, V)>,
        deletes: Vec<(&'static str, K)>,
    ) -> Result<(), Self::Error> {
        let mut write_batch = WriteBatch::new();
        for (cf, key, value) in updates {
            write_batch.put_cf(&self.cf(cf), key.encode_to_vec(), value.encode_to_vec());
        }
        for (cf, key) in deletes {
            write_batch.delete_cf(&self.cf(cf), key.encode_to_vec());
        }
        self.write(write_batch)?;
        Ok(())
    }
}

pub trait AsyncPersister<K, V>
where
    K: Message + Default,
    V: Message + Default,
{
    type Error: Error;
    type Iter<'a>: Iterator<Item = Result<(K, V), Self::Error>> + 'a
    where
        Self: 'a,
        K: 'a,
        V: 'a;

    async fn save(
        &self,
        updates: Vec<(&'static str, K, V)>,
        deletes: Vec<(&'static str, K)>,
    ) -> Result<(), Self::Error>;

    async fn load(&self, cf: &'static str, key: K) -> Result<Option<V>, Self::Error>;

    fn load_prefix_iter<'a>(
        &'a self,
        cf: &'static str,
        prefix: &[u8],
    ) -> Result<Self::Iter<'a>, Self::Error>
    where
        K: 'a,
        V: 'a;

    fn load_all_iter<'a>(&'a self, cf: &'static str) -> Result<Self::Iter<'a>, Self::Error>
    where
        K: 'a,
        V: 'a;
}

enum Command<K, V, E> {
    Save {
        reply: oneshot::Sender<Result<(), E>>,
        arg: (Vec<(&'static str, K, V)>, Vec<(&'static str, K)>),
    },
    Load {
        reply: oneshot::Sender<Result<Option<V>, E>>,
        arg: (&'static str, K),
    },
    Close,
}

struct Inner {
    db: DB,
    tx: mpsc::Sender<Command<Vec<u8>, Vec<u8>, PersisterError<rocksdb::Error>>>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for Inner {
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
pub struct Database {
    inner: Arc<Inner>,
}

impl<K, V> AsyncPersister<K, V> for Database
where
    K: Message + Default,
    V: Message + Default,
{
    type Error = PersisterError<rocksdb::Error>;
    type Iter<'a>
        = Box<dyn Iterator<Item = Result<(K, V), Self::Error>> + 'a>
    where
        Self: 'a,
        K: 'a,
        V: 'a;

    async fn save(
        &self,
        updates: Vec<(&'static str, K, V)>,
        deletes: Vec<(&'static str, K)>,
    ) -> Result<(), Self::Error> {
        let (tx, rx) = oneshot::channel();
        let updates = updates
            .into_iter()
            .map(|(cf, key, value)| (cf, key.encode_to_vec(), value.encode_to_vec()))
            .collect();
        let deletes = deletes
            .into_iter()
            .map(|(cf, key)| (cf, key.encode_to_vec()))
            .collect();
        self.inner
            .tx
            .send(Command::Save {
                reply: tx,
                arg: (updates, deletes),
            })
            .await
            .expect("failed to send command");
        rx.await.expect("failed to receive reply")
    }

    async fn load(&self, cf: &'static str, key: K) -> Result<Option<V>, Self::Error> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .tx
            .send(Command::Load {
                reply: tx,
                arg: (cf, key.encode_to_vec()),
            })
            .await
            .expect("failed to send command");
        let value = rx.await.expect("failed to receive reply")?;
        let Some(value) = value else {
            return Ok(None);
        };
        let value = V::decode(&value[..]).map_err(Self::Error::DecodeValueError)?;
        Ok(Some(value))
    }

    fn load_prefix_iter<'a>(
        &'a self,
        cf: &'static str,
        prefix: &[u8],
    ) -> Result<Self::Iter<'a>, Self::Error>
    where
        K: 'a,
        V: 'a,
    {
        self.inner.db.load_prefix_iter(cf, prefix)
    }

    fn load_all_iter<'a>(&'a self, cf: &'static str) -> Result<Self::Iter<'a>, Self::Error>
    where
        K: 'a,
        V: 'a,
    {
        self.inner.db.load_all_iter(cf)
    }
}

impl Database {
    pub fn new(path: &'static str) -> Result<Self, PersisterError<rocksdb::Error>> {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);

        // Open with ALL column families - don't be an amateur
        let cfs = vec!["default", "order_book", "all_orders", "meta"];
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
        let bind_inner = inner.clone();
        let handle = thread::spawn(move || {
            while let Some(command) = rx.blocking_recv() {
                match command {
                    Command::Close => break,
                    Command::Save { reply, arg } => {
                        let res = bind_inner.db.save(arg.0, arg.1);
                        if let Err(e) = reply.send(res) {
                            tracing::warn!("failed to send reply: {:?}", e);
                        }
                    }
                    Command::Load { reply, arg } => {
                        let (cf, key) = arg;
                        let value = bind_inner.db.load(cf, key);
                        if let Err(e) = reply.send(value) {
                            tracing::warn!("failed to send reply: {:?}", e);
                        }
                    }
                }
            }
            tracing::info!("persister thread stopped");
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
