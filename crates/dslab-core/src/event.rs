//! Simulation events.

use std::cmp::Ordering;

use downcast_rs::{impl_downcast, Downcast};
use dyn_clone::{clone_trait_object, DynClone};
use serde::ser::Serialize;

use crate::component::Id;

/// Event identifier.
pub type EventId = u64;

/// Trait that should be implemented by event payload.
pub trait EventData: Downcast + DynClone + erased_serde::Serialize {}

impl_downcast!(EventData);

clone_trait_object!(EventData);

erased_serde::serialize_trait_object!(EventData);

impl<T: Serialize + DynClone + 'static> EventData for T {}

/// Representation of event.
#[derive(Clone)]
pub struct Event {
    /// Unique event identifier.
    ///
    /// Events are numbered sequentially starting from 0.
    pub id: EventId,
    /// Time of event occurrence.
    pub time: f64,
    /// Identifier of event source.
    pub src: Id,
    /// Identifier of event destination.
    pub dest: Id,
    /// Event payload.
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
