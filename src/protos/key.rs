use std::cmp::Ordering;

use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimebasedKey([u8; 16]);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PricebasedKey([u8; 16]);

impl PartialOrd for TimebasedKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TimebasedKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl PartialOrd for PricebasedKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PricebasedKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl TimebasedKey {
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    pub fn get_price(&self) -> f32 {
        let mut price = [0; 4];
        price.copy_from_slice(&self.0[10..14]);
        f32::from_be_bytes(price)
    }

    pub fn to_bytes(&self) -> [u8; 16] {
        self.0
    }
}

impl PricebasedKey {
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    pub fn get_price(&self) -> f32 {
        let mut price = [0; 4];
        price.copy_from_slice(&self.0[0..4]);
        f32::from_be_bytes(price)
    }

    pub fn to_bytes(&self) -> [u8; 16] {
        self.0
    }
}

impl From<TimebasedKey> for PricebasedKey {
    fn from(key: TimebasedKey) -> Self {
        let mut bytes = [0; 16];
        bytes[0..6].copy_from_slice(&key.0[10..16]);
        bytes[6..16].copy_from_slice(&key.0[0..10]);
        Self(bytes)
    }
}

impl From<PricebasedKey> for TimebasedKey {
    fn from(key: PricebasedKey) -> Self {
        let mut bytes = [0; 16];
        bytes[0..10].copy_from_slice(&key.0[6..16]);
        bytes[10..16].copy_from_slice(&key.0[0..6]);
        Self(bytes)
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum OrderKey {
    TimeBased(TimebasedKey),
    PriceBased(PricebasedKey),
}

impl OrderKey {
    pub fn new(price: f32) -> Self {
        let bytes = price.to_be_bytes();
        let node_id = [bytes[0], bytes[1], bytes[2], bytes[3], 0, 0];
        let uuid = Uuid::now_v6(&node_id);
        Self::TimeBased(TimebasedKey(uuid.into_bytes()))
    }

    pub fn timebased_bytes(&self) -> [u8; 16] {
        match self {
            Self::TimeBased(bytes) => bytes.0,
            Self::PriceBased(bytes) => {
                let mut new = [0; 16];
                new[0..10].copy_from_slice(&bytes.0[6..16]);
                new[10..16].copy_from_slice(&bytes.0[0..6]);
                new
            }
        }
    }

    #[allow(dead_code)]
    pub fn timebased(&self) -> TimebasedKey {
        match self {
            Self::TimeBased(inner) => *inner,
            Self::PriceBased(_) => TimebasedKey(self.timebased_bytes()),
        }
    }

    pub fn pricebased_bytes(&self) -> [u8; 16] {
        match self {
            Self::TimeBased(bytes) => {
                let mut new = [0; 16];
                new[0..6].copy_from_slice(&bytes.0[10..16]);
                new[6..16].copy_from_slice(&bytes.0[0..10]);
                new
            }
            Self::PriceBased(bytes) => bytes.0,
        }
    }

    pub fn pricebased(&self) -> PricebasedKey {
        match self {
            Self::TimeBased(_) => PricebasedKey(self.pricebased_bytes()),
            Self::PriceBased(inner) => *inner,
        }
    }

    pub fn from_timebased(bytes: [u8; 16]) -> Self {
        Self::TimeBased(TimebasedKey(bytes))
    }

    #[allow(dead_code)]
    pub fn from_pricebased(bytes: [u8; 16]) -> Self {
        Self::PriceBased(PricebasedKey(bytes))
    }

    #[allow(dead_code)]
    pub fn get_price(&self) -> f32 {
        match self {
            Self::TimeBased(key) => key.get_price(),
            Self::PriceBased(key) => key.get_price(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use crate::protos::{Key, OrderKey};
    use quickcheck_macros::quickcheck;

    #[quickcheck]
    fn convert(order_key: OrderKey) {
        assert_eq!(
            order_key.timebased().get_price(),
            order_key.pricebased().get_price()
        );

        assert_eq!(
            order_key,
            OrderKey::TimeBased(OrderKey::PriceBased(order_key.pricebased()).timebased())
        );
    }

    #[quickcheck]
    fn compare(order_key1: OrderKey, order_key2: OrderKey) {
        let order_key1 = order_key1.pricebased();
        let order_key2 = order_key2.pricebased();
        let total = order_key1.cmp(&order_key2);
        let price = order_key1
            .get_price()
            .partial_cmp(&order_key2.get_price())
            .unwrap();

        if total.is_eq() {
            assert_eq!(price, Ordering::Equal);
        } else if total.is_gt() {
            assert!(
                price == Ordering::Greater || price == Ordering::Equal,
                "{} > {}",
                order_key1.get_price(),
                order_key2.get_price()
            );
        } else {
            assert!(
                price == Ordering::Less || price == Ordering::Equal,
                "{} < {}",
                order_key1.get_price(),
                order_key2.get_price()
            );
        }
    }

    #[quickcheck]
    fn order_key_to_proto(order_key: OrderKey) {
        let proto: Key = order_key.into();
        let key: OrderKey = proto.into();
        assert_eq!(key, order_key);
    }
}
