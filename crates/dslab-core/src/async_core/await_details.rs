//! Contains public interfaces of async_core functionality.

use super::shared_state::EmptyData;
use crate::{event::EventData, Event, Id};

/// Type of key that represents the details of event to wait for.
pub type DetailsKey = u64;

/// Represents the result of `SimulationContext::async_wait_event_for` call.
pub enum AwaitResult<T: EventData> {
    /// Contains Event with destination and source that it was waited from. Id, time, and data are empty.
    Timeout(Event),
    /// Contains full event without data, and data of specific type separately.
    Ok((Event, T)),
}

impl<T: EventData> Default for AwaitResult<T> {
    fn default() -> Self {
        Self::Timeout(Event {
            id: 0,
            time: 0.,
            src: 0,
            dst: 0,
            data: Box::new(EmptyData {}),
        })
    }
}

impl<T: EventData> AwaitResult<T> {
    /// Creates a Timeout result with specified source and destination.
    pub(crate) fn timeout_with(src: Id, dst: Id) -> Self {
        Self::Timeout(Event {
            id: 0,
            time: 0.,
            src,
            dst,
            data: Box::new(EmptyData {}),
        })
    }
}
