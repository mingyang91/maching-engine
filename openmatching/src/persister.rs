use std::error::Error;

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

    #[allow(dead_code)]
    fn load_all_iter<'a>(&'a self, cf: &'static str) -> Result<Self::Iter<'a>, Self::Error>
    where
        V: 'a;
}
