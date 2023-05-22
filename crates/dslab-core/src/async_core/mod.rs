#![doc = include_str!("../../readme.md")]

use crate::async_core;
pub(crate) mod build_macro_rules;

async_core! {
    pub mod await_details;
    pub mod sync;

    pub(crate) mod executor;
    pub(crate) mod shared_state;
    pub(crate) mod task;
    pub(crate) mod timer;
    pub(crate) mod waker;
}
