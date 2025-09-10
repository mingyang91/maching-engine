use std::{cmp::Ordering, error::Error};

use prost::Message;
use quickcheck::{Arbitrary, Gen};
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

impl Arbitrary for Side {
    fn arbitrary(g: &mut Gen) -> Self {
        *g.choose(&[Side::Buy, Side::Sell])
            .expect("should choose side")
    }
}

impl Arbitrary for OrderType {
    fn arbitrary(g: &mut Gen) -> Self {
        *g.choose(&[
            OrderType::Limit,
            OrderType::Market,
            OrderType::Stop,
            OrderType::StopLimit,
        ])
        .expect("should choose order type")
    }
}

impl Arbitrary for OrderStatus {
    fn arbitrary(g: &mut Gen) -> Self {
        *g.choose(&[
            OrderStatus::Open,
            OrderStatus::PartiallyFilled,
            OrderStatus::Filled,
            OrderStatus::Cancelled,
        ])
        .expect("should choose order status")
    }
}

impl Arbitrary for ProtoUuid {
    fn arbitrary(g: &mut Gen) -> Self {
        let uuid = Uuid::now_v6(&[u8::arbitrary(g); 6]);
        uuid.into()
    }
}

impl Arbitrary for Key {
    fn arbitrary(g: &mut Gen) -> Self {
        Key {
            price: gaussian(g, 100.0, 100.0),
            uuid: Some(ProtoUuid::arbitrary(g)),
        }
    }
}

impl Arbitrary for Order {
    fn arbitrary(g: &mut Gen) -> Self {
        let quantity = log_gaussian(g, 100.0, 2.48).ceil() as u64 + 1;
        Order {
            key: Some(Key::arbitrary(g)),
            side: Side::arbitrary(g).into(),
            order_type: OrderType::arbitrary(g).into(),
            quantity,
            remaining: quantity,
            status: OrderStatus::Open.into(),
        }
    }
}

fn gaussian(g: &mut Gen, mean: f32, std_dev: f32) -> f32 {
    let x = f32::arbitrary(g);
    let x = match x {
        -1.0..=1.0 => x,
        f32::INFINITY => 1.0,
        f32::NEG_INFINITY => -1.0,
        _ if x.is_nan() => 0.0,
        _ => x / f32::MAX,
    };
    let x = if x < 0.0 {
        -x.abs().sqrt()
    } else {
        x.abs().sqrt()
    };
    let res = mean + std_dev * x;
    if res == 0.0 { 1e-10 } else { res }
}

fn log_gaussian(g: &mut Gen, mu: f32, sigma: f32) -> f32 {
    let y = gaussian(g, mu, sigma);
    y.exp()
}
