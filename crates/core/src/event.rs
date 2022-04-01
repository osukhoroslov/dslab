use std::cmp::Ordering;

use downcast_rs::{impl_downcast, Downcast};
use serde::ser::Serialize;

pub trait EventData: Downcast + erased_serde::Serialize {}

impl_downcast!(EventData);

erased_serde::serialize_trait_object!(EventData);

impl<T: Serialize + 'static> EventData for T {}

pub struct Event {
    pub id: u64,
    pub time: f64,
    pub src: u32,
    pub dest: u32,
    pub data: Box<dyn EventData>,
}

impl Eq for Event {}

impl PartialEq for Event {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Ord for Event {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .time
            .partial_cmp(&self.time)
            .unwrap()
            .then_with(|| other.id.cmp(&self.id))
    }
}

impl PartialOrd for Event {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
