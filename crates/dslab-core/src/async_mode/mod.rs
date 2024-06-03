//! Asynchronous programming support.

#![warn(unsafe_op_in_unsafe_fn)]

use crate::async_mode_enabled;
pub(crate) mod macros;

async_mode_enabled!(
    pub mod event_future;
    pub mod queue;
    pub mod timer_future;

    pub(crate) mod channel;
    pub(crate) mod executor;
    pub(crate) mod promise_store;
    pub(crate) mod task;

    mod waker;

    pub use event_future::{AwaitResult, EventFuture, EventKey};
    pub use timer_future::TimerFuture;
    pub use queue::UnboundedQueue;
);
