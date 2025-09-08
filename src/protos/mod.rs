use std::cmp::Ordering;

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
