use std::cmp::Ordering;

use decorum::R64;
use downcast_rs::{impl_downcast, Downcast};
use serde::ser::{Serialize, SerializeStruct, Serializer};
use serde_type_name::type_name;

pub trait EventData: Downcast + erased_serde::Serialize {}

impl_downcast!(EventData);

erased_serde::serialize_trait_object!(EventData);

impl<T: Serialize + 'static> EventData for T {}

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

impl Serialize for Event {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("Event", 3)?;
        state.serialize_field("type", type_name(&self.data).unwrap())?;
        state.serialize_field("data", &self.data)?;
        state.serialize_field("src", &self.src)?;
        state.end()
    }
}
