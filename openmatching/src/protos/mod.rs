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

impl From<TimebasedKey> for Key {
    fn from(key: TimebasedKey) -> Self {
        let bytes = key.to_bytes();
        let mut high_bytes = [0; 8];
        high_bytes.copy_from_slice(&bytes[0..8]);
        let high = u64::from_le_bytes(high_bytes);
        let mut low_bytes = [0; 8];
        low_bytes.copy_from_slice(&bytes[8..16]);
        let low = u64::from_le_bytes(low_bytes);
        Key { high, low }
    }
}

impl From<Key> for TimebasedKey {
    fn from(key: Key) -> Self {
        let mut bytes = [0; 16];
        bytes[0..8].copy_from_slice(&key.high.to_le_bytes());
        bytes[8..16].copy_from_slice(&key.low.to_le_bytes());

        TimebasedKey::from_bytes(bytes)
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
        *g.choose(&[OrderType::Limit, OrderType::Market])
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

impl Arbitrary for TimebasedKey {
    fn arbitrary(g: &mut Gen) -> Self {
        let price = gaussian(g, 100.0, 100.0);
        Self::new(price)
    }
}

impl Arbitrary for Key {
    fn arbitrary(g: &mut Gen) -> Self {
        TimebasedKey::arbitrary(g).into()
    }
}

// Helper methods for Order to extract common fields
impl Order {
    pub fn is_buy(&self) -> bool {
        matches!(self.side_data, Some(order::SideData::Buy(_)))
    }

    pub fn is_sell(&self) -> bool {
        matches!(self.side_data, Some(order::SideData::Sell(_)))
    }

    pub fn side(&self) -> Side {
        if self.is_buy() { Side::Buy } else { Side::Sell }
    }

    pub fn order_type(&self) -> OrderType {
        match &self.side_data {
            Some(order::SideData::Buy(b)) => b.order_type(),
            Some(order::SideData::Sell(s)) => s.order_type(),
            None => OrderType::Limit, // default
        }
    }

    pub fn price(&self) -> f32 {
        match &self.side_data {
            Some(order::SideData::Buy(b)) => b.limit_price,
            Some(order::SideData::Sell(s)) => s.limit_price,
            None => 0.0,
        }
    }

    // For compatibility - returns quantity/target based on side
    pub fn quantity(&self) -> u64 {
        match &self.side_data {
            Some(order::SideData::Buy(b)) => b.target_quantity,
            Some(order::SideData::Sell(s)) => s.total_quantity,
            None => 0,
        }
    }

    // For compatibility - returns remaining based on side
    pub fn remaining(&self) -> u64 {
        match &self.side_data {
            Some(order::SideData::Buy(b)) => b.target_quantity - b.filled_quantity,
            Some(order::SideData::Sell(s)) => s.remaining_quantity,
            None => 0,
        }
    }
}

impl Arbitrary for Order {
    fn arbitrary(g: &mut Gen) -> Self {
        let side = *g.choose(&[true, false]).unwrap(); // true = buy, false = sell
        let order_type = OrderType::arbitrary(g);

        let side_data = if side {
            // Create BuyOrder
            let target_quantity = log_gaussian(g, 100.0, 2.48).ceil() as u64;
            let target_quantity = target_quantity.clamp(1, 100000);

            let limit_price = match order_type {
                OrderType::Limit => gaussian(g, 100.0, 100.0),
                OrderType::Market => 0.0,
            };

            let total_funds = if order_type == OrderType::Market {
                // Market buy: allocate generous funds
                target_quantity as f32 * 200.0
            } else {
                // Limit buy: exact funds needed
                target_quantity as f32 * limit_price
            };

            Some(order::SideData::Buy(BuyOrder {
                order_type: order_type.into(),
                limit_price,
                total_funds,
                funds_remaining: total_funds,
                target_quantity,
                filled_quantity: 0,
            }))
        } else {
            // Create SellOrder
            let total_quantity = log_gaussian(g, 100.0, 2.48).ceil() as u64;
            let total_quantity = total_quantity.clamp(1, 100000);

            let limit_price = match order_type {
                OrderType::Limit => gaussian(g, 100.0, 100.0),
                OrderType::Market => 0.0,
            };

            Some(order::SideData::Sell(SellOrder {
                order_type: order_type.into(),
                limit_price,
                total_quantity,
                remaining_quantity: total_quantity,
                total_proceeds: 0.0,
            }))
        };

        Order {
            key: Some(Key::arbitrary(g)),
            status: OrderStatus::Open.into(),
            side_data,
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
