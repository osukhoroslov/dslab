//! Asynchronous programming support.

use crate::async_enabled;
pub(crate) mod build_macro_rules;

async_enabled! {
    pub mod await_details;
    pub mod sync;

    pub(crate) mod executor;
    pub(crate) mod shared_state;
    pub(crate) mod task;
    pub(crate) mod timer;
    pub(crate) mod waker;

    pub use await_details::EventKey;
    pub use await_details::AwaitResult;
}
