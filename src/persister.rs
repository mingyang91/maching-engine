use std::error::Error;

use prost::Message;
use rocksdb::{DB, IteratorMode, ReadOptions, WriteBatch, WriteOptions};

const META_PREFIX: &str = "__meta_";
const LAST_SEQUENCE_KEY: &str = "__meta_last_sequence";

// Separate concerns - metadata doesn't belong with typed operations
pub trait MetadataStore {
    type Error: Error;
    fn last_sequence(&self) -> Result<u64, Self::Error>;
    fn put_last_sequence(&self, sequence: u64) -> Result<(), Self::Error>;
}

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

    fn save(&self, key: K, value: V) -> Result<(), Self::Error>;
    fn save_batch(&self, updates: Vec<(K, V)>, deletes: Vec<K>) -> Result<(), Self::Error>;
    fn load(&self, key: K) -> Result<Option<V>, Self::Error>;
    fn load_prefix_iter<'a>(&'a self, prefix: &[u8]) -> Result<Self::Iter<'a>, Self::Error>
    where
        K: 'a,
        V: 'a;
    fn load_range_iter<'a>(&'a self, start: K, end: K) -> Result<Self::Iter<'a>, Self::Error>
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

// Simple, direct implementation for metadata - no generics needed
impl MetadataStore for DB {
    type Error = PersisterError<rocksdb::Error>;
    fn last_sequence(&self) -> Result<u64, Self::Error> {
        let Some(value) = self.get(LAST_SEQUENCE_KEY.as_bytes())? else {
            return Ok(0);
        };
        u64::decode(&value[..]).map_err(PersisterError::DecodeSequenceError)
    }

    fn put_last_sequence(&self, sequence: u64) -> Result<(), Self::Error> {
        self.put(LAST_SEQUENCE_KEY.as_bytes(), &sequence.encode_to_vec())?;
        Ok(())
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

    fn save(&self, key: K, value: V) -> Result<(), Self::Error> {
        self.put(key.encode_to_vec(), value.encode_to_vec())?;
        Ok(())
    }

    fn save_batch(&self, updates: Vec<(K, V)>, deletes: Vec<K>) -> Result<(), Self::Error> {
        let mut sequence = self.last_sequence()?;
        let mut write_batch = WriteBatch::new();
        for (key, value) in updates {
            sequence = sequence + 1;
            write_batch.put(key.encode_to_vec(), value.encode_to_vec());
        }
        for key in deletes {
            write_batch.delete(key.encode_to_vec());
        }
        write_batch.put(LAST_SEQUENCE_KEY.as_bytes(), &sequence.encode_to_vec());
        let mut write_options = WriteOptions::default();
        write_options.set_sync(true);
        self.write_opt(write_batch, &write_options)?;
        println!("last sequence: {}", sequence);
        Ok(())
    }

    fn load(&self, key: K) -> Result<Option<V>, Self::Error> {
        let Some(value) = self.get(key.encode_to_vec())? else {
            return Ok(None);
        };

        let value = V::decode(&value[..]).map_err(Self::Error::DecodeValueError)?;
        Ok(Some(value))
    }

    fn load_prefix_iter<'a>(&'a self, prefix: &[u8]) -> Result<Self::Iter<'a>, Self::Error>
    where
        K: 'a,
        V: 'a,
    {
        let iter = self.prefix_iterator(prefix).map(|result| {
            let (key, value) = result?;
            let key = K::decode(&key[..]).map_err(Self::Error::DecodeKeyError)?;
            let value = V::decode(&value[..]).map_err(Self::Error::DecodeValueError)?;
            Ok((key, value))
        });
        Ok(Box::new(iter))
    }

    fn load_range_iter<'a>(&'a self, start: K, end: K) -> Result<Self::Iter<'a>, Self::Error>
    where
        K: 'a,
        V: 'a,
    {
        let mut readopts = ReadOptions::default();
        readopts.set_iterate_upper_bound(end.encode_to_vec());
        readopts.set_iterate_lower_bound(start.encode_to_vec());
        let iter = self
            .iterator_opt(IteratorMode::Start, readopts)
            .map(|result| {
                let (key, value) = result?;
                let key = K::decode(&key[..]).map_err(Self::Error::DecodeKeyError)?;
                let value = V::decode(&value[..]).map_err(Self::Error::DecodeValueError)?;
                Ok((key, value))
            });
        Ok(Box::new(iter))
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
        let db = DB::open(&options, "test.db").unwrap();
        db.put_last_sequence(100).unwrap();
        assert_eq!(db.last_sequence().unwrap(), 100);
    }
}
