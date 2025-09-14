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
    pub fn new(price: f32) -> Self {
        let bytes = price.to_be_bytes();
        let node_id = [bytes[0], bytes[1], bytes[2], bytes[3], 0, 0];
        let uuid = Uuid::now_v6(&node_id);
        Self(uuid.into_bytes())
    }

    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    #[allow(dead_code)]
    pub fn get_price(&self) -> f32 {
        let mut price = [0; 4];
        price.copy_from_slice(&self.0[10..14]);
        f32::from_be_bytes(price)
    }

    pub fn to_bytes(&self) -> [u8; 16] {
        self.0
    }

    pub fn to_pricebased(&self) -> PricebasedKey {
        let mut bytes = [0; 16];
        bytes[0..6].copy_from_slice(&self.0[10..16]);
        bytes[6..16].copy_from_slice(&self.0[0..10]);
        PricebasedKey(bytes)
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

    pub fn to_timebased(&self) -> TimebasedKey {
        let mut bytes = [0; 16];
        bytes[0..10].copy_from_slice(&self.0[6..16]);
        bytes[10..16].copy_from_slice(&self.0[0..6]);
        TimebasedKey(bytes)
    }
}

impl From<TimebasedKey> for PricebasedKey {
    fn from(key: TimebasedKey) -> Self {
        key.to_pricebased()
    }
}

impl From<PricebasedKey> for TimebasedKey {
    fn from(key: PricebasedKey) -> Self {
        key.to_timebased()
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use crate::protos::{Key, TimebasedKey};
    use quickcheck_macros::quickcheck;

    #[quickcheck]
    fn convert(order_key: TimebasedKey) {
        assert_eq!(order_key.get_price(), order_key.to_pricebased().get_price());

        assert_eq!(order_key, order_key.to_pricebased().to_timebased());
    }

    #[quickcheck]
    fn compare(order_key1: TimebasedKey, order_key2: TimebasedKey) {
        let order_key1 = order_key1.to_pricebased();
        let order_key2 = order_key2.to_pricebased();
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
    fn order_key_to_proto(order_key: TimebasedKey) {
        let proto: Key = order_key.into();
        let key: TimebasedKey = proto.into();
        assert_eq!(key, order_key);
    }
}
