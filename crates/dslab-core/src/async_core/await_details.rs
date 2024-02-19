//! Contains public interfaces of async_core functionality.

use crate::{event::EventData, Event, Id};

/// Type of key that represents the specific details of event to wait for.
pub type EventKey = u64;

/// Represents the result of `EventFuture::with_timeout`.
pub enum AwaitResult<T: EventData> {
    /// Contains Event with destination and source that it was waited from. Id, time, and data are empty.
    Timeout(TimeoutInfo),
    /// Contains full event without data, and data of specific type separately.
    Ok((Event, T)),
}

/// Represents the result of `EventFuture::with_timeout` if timeout fired.
pub struct TimeoutInfo {
    /// Timeout that was set up for the EventFuture.
    pub timeout: f64,
    /// EventKey of the requested Event (None if it was received from getter without EventKey).
    pub event_key: Option<EventKey>,
    /// Id of the component that was supposed to send the Event (None if it was not specified).
    pub src: Option<Id>,
}
