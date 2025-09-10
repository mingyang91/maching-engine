use std::{cmp::Ordering, error::Error};

use prost::Message;
use uuid::Uuid;

include!(concat!(env!("OUT_DIR"), "/matching_engine.protos.rs"));

impl PartialOrd for ProtoUuid {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ProtoUuid {
    fn cmp(&self, other: &Self) -> Ordering {
        self.high.cmp(&other.high).then(self.low.cmp(&other.low))
    }
}

impl From<ProtoUuid> for Uuid {
    fn from(uuid: ProtoUuid) -> Self {
        Uuid::from_u64_pair(uuid.high, uuid.low)
    }
}

impl From<Uuid> for ProtoUuid {
    fn from(uuid: Uuid) -> Self {
        let (high, low) = uuid.as_u64_pair();
        ProtoUuid { high, low }
    }
}

impl Eq for Key {}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> Ordering {
        let price_cmp = self
            .price
            .partial_cmp(&other.price)
            .expect("price should be comparable");
        if price_cmp != Ordering::Equal {
            return price_cmp;
        }

        self.uuid.cmp(&other.uuid)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(C)]
pub struct FixedKey {
    pub price: f32,
    pub uuid: Uuid,
}

impl Eq for FixedKey {}

impl PartialOrd for FixedKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FixedKey {
    fn cmp(&self, other: &Self) -> Ordering {
        let price_cmp = self
            .price
            .partial_cmp(&other.price)
            .expect("price should be comparable");
        if price_cmp != Ordering::Equal {
            return price_cmp;
        }

        self.uuid.cmp(&other.uuid)
    }
}

impl From<Key> for FixedKey {
    fn from(key: Key) -> Self {
        FixedKey {
            price: key.price,
            uuid: key.uuid.expect("uuid should be present").into(),
        }
    }
}

impl From<FixedKey> for Key {
    fn from(key: FixedKey) -> Self {
        Key {
            price: key.price,
            uuid: Some(key.uuid.into()),
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
