mod key;

use std::{cmp::Ordering, error::Error};

pub use key::*;
use prost::Message;
use quickcheck::{Arbitrary, Gen};

include!(concat!(env!("OUT_DIR"), "/matching_engine.protos.rs"));

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> Ordering {
        self.high.cmp(&other.high).then(self.low.cmp(&other.low))
    }
}

impl From<OrderKey> for Key {
    fn from(key: OrderKey) -> Self {
        let bytes = key.timebased();
        let mut high_bytes = [0; 8];
        high_bytes.copy_from_slice(&bytes[0..8]);
        let high = u64::from_le_bytes(high_bytes);
        let mut low_bytes = [0; 8];
        low_bytes.copy_from_slice(&bytes[8..16]);
        let low = u64::from_le_bytes(low_bytes);
        Key { high, low }
    }
}

impl From<Key> for OrderKey {
    fn from(key: Key) -> Self {
        let mut bytes = [0; 16];
        bytes[0..8].copy_from_slice(&key.high.to_le_bytes());
        bytes[8..16].copy_from_slice(&key.low.to_le_bytes());

        OrderKey::from_timebased(bytes)
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

impl Arbitrary for OrderKey {
    fn arbitrary(g: &mut Gen) -> Self {
        let price = gaussian(g, 100.0, 100.0);
        Self::new(price)
    }
}

impl Arbitrary for Order {
    fn arbitrary(g: &mut Gen) -> Self {
        let quantity = log_gaussian(g, 100.0, 2.48).ceil() as u64 + 1;
        Order {
            key: Some(OrderKey::arbitrary(g).into()),
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
