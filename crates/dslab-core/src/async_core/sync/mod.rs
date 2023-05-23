//! synchronization primitives

use crate::async_enabled;

async_enabled! {
    pub mod queue;
}
