use std::{cmp::Ordering, error::Error};

use prost::Message;

include!(concat!(env!("OUT_DIR"), "/matching_engine.protos.rs"));

// Only implement what prost doesn't generate - custom ordering
impl Eq for Key {}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // Simple, clear comparison logic
        match self.price.partial_cmp(&other.price)? {
            Ordering::Equal => match self.timestamp.cmp(&other.timestamp) {
                Ordering::Equal => Some(self.sequence.cmp(&other.sequence)),
                other => Some(other),
            },
            other => Some(other),
        }
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> Ordering {
        // Keys with valid prices should always be comparable
        self.partial_cmp(other).expect("keys should be comparable")
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(C)]
pub struct FixedKey {
    pub price: f32,
    pub timestamp: u64,
    pub sequence: u32,
}

impl Eq for FixedKey {}

impl PartialOrd for FixedKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.price.partial_cmp(&other.price)? {
            Ordering::Equal => match self.timestamp.cmp(&other.timestamp) {
                Ordering::Equal => Some(self.sequence.cmp(&other.sequence)),
                other => Some(other),
            },
            other => Some(other),
        }
    }
}

impl Ord for FixedKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).expect("keys should be comparable")
    }
}

impl From<Key> for FixedKey {
    fn from(key: Key) -> Self {
        FixedKey {
            price: key.price,
            timestamp: key.timestamp,
            sequence: key.sequence,
        }
    }
}

impl From<FixedKey> for Key {
    fn from(key: FixedKey) -> Self {
        Key {
            price: key.price,
            timestamp: key.timestamp,
            sequence: key.sequence,
        }
    }
}

impl AsRef<[u8]> for FixedKey {
    fn as_ref(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self as *const Self as *const u8,
                std::mem::size_of::<Self>(),
            )
        }
    }
}

impl TryFrom<&[u8]> for FixedKey {
    type Error = std::io::Error;
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() != std::mem::size_of::<Self>() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid key length",
            ));
        }
        Ok(unsafe { std::ptr::read(value.as_ptr() as *const Self) })
    }
}

// impl<T> TryFromBytes for T
// where
//     T: TryFrom<&'static [u8]> + Send + Sync + 'static,
// {
//     type Error = T::Error;
//     fn try_from_bytes(bytes: &'static [u8]) -> Result<Self, Self::Error> {
//         Self::try_from(bytes)
//     }
// }

// impl<T> ToBytes for T
// where
//     T: AsRef<[u8]> + Send + Sync + 'static,
// {
//     fn to_bytes(&self) -> Vec<u8> {
//         self.as_ref().to_vec()
//     }
// }

pub trait ToBytes: Send + Sync {
    fn to_bytes(&self) -> Vec<u8>;
}

impl<T> ToBytes for T
where
    T: Message + Send + Sync,
{
    fn to_bytes(&self) -> Vec<u8> {
        self.encode_to_vec()
    }
}

impl ToBytes for FixedKey {
    fn to_bytes(&self) -> Vec<u8> {
        self.as_ref().to_vec()
    }
}

impl TryFromBytes for FixedKey {
    type Error = std::io::Error;
    fn try_from_bytes(bytes: &[u8]) -> Result<Self, Self::Error> {
        Self::try_from(bytes)
    }
}

pub trait TryFromBytes: Sized + Send + Sync {
    type Error: Error + Send + Sync + 'static;
    fn try_from_bytes(bytes: &[u8]) -> Result<Self, Self::Error>;
}

impl<T> TryFromBytes for T
where
    T: Message + Default + Sized + Send + Sync,
{
    type Error = prost::DecodeError;
    fn try_from_bytes(bytes: &[u8]) -> Result<Self, Self::Error> {
        T::decode(bytes)
    }
}
