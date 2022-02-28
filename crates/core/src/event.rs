use std::cmp::Ordering;
use std::fmt::Debug;

use decorum::R64;
use downcast_rs::{impl_downcast, Downcast};

pub trait EventData: Downcast + Debug {}

impl_downcast!(EventData);

impl<T: Debug + 'static> EventData for T {}

#[derive(Debug)]
pub struct Event {
    pub id: u64,
    pub time: R64,
    pub src: String,
    pub dest: String,
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
        other.time.cmp(&self.time).then_with(|| other.id.cmp(&self.id))
    }
}

impl PartialOrd for Event {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
