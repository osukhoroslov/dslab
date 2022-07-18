use std::cmp::Ordering;

use downcast_rs::{impl_downcast, Downcast};
use serde::ser::Serialize;

use crate::component::Id;

pub type EventId = u64;

pub trait EventData: Downcast + erased_serde::Serialize {}

impl_downcast!(EventData);

erased_serde::serialize_trait_object!(EventData);

impl<T: Serialize + 'static> EventData for T {}

pub struct Event {
    pub id: EventId,
    pub time: f64,
    pub src: Id,
    pub dest: Id,
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
        other.time.total_cmp(&self.time).then_with(|| other.id.cmp(&self.id))
    }
}

impl PartialOrd for Event {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
