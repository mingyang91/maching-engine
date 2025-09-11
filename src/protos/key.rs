use uuid::Uuid;

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum OrderKey {
    TimeBased([u8; 16]),
    PriceBased([u8; 16]),
}

impl OrderKey {
    pub fn new(price: f32) -> Self {
        let bytes = price.to_le_bytes();
        let node_id = [bytes[0], bytes[1], bytes[2], bytes[3], 0, 0];
        let uuid = Uuid::now_v6(&node_id);
        Self::TimeBased(uuid.into_bytes())
    }

    pub fn timebased(&self) -> [u8; 16] {
        match self {
            Self::TimeBased(bytes) => *bytes,
            Self::PriceBased(bytes) => {
                let mut new = [0; 16];
                new[0..6].copy_from_slice(&bytes[10..16]);
                new[6..16].copy_from_slice(&bytes[0..10]);
                new
            }
        }
    }

    pub fn pricebased(&self) -> [u8; 16] {
        match self {
            Self::TimeBased(bytes) => {
                let mut new = [0; 16];
                new[0..10].copy_from_slice(&bytes[6..16]);
                new[10..16].copy_from_slice(&bytes[0..6]);
                new
            }
            Self::PriceBased(bytes) => *bytes,
        }
    }

    pub fn from_timebased(bytes: [u8; 16]) -> Self {
        Self::TimeBased(bytes)
    }

    pub fn from_pricebased(bytes: [u8; 16]) -> Self {
        Self::PriceBased(bytes)
    }

    pub fn get_price(&self) -> f32 {
        match self {
            Self::TimeBased(bytes) => {
                let mut price = [0; 4];
                price.copy_from_slice(&bytes[10..14]);
                f32::from_le_bytes(price)
            }
            Self::PriceBased(bytes) => {
                let mut price = [0; 4];
                price.copy_from_slice(&bytes[0..4]);
                f32::from_le_bytes(price)
            }
        }
    }
}
