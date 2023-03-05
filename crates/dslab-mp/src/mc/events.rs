use serde::Serialize;

use crate::message::Message;

#[derive(Serialize, Clone)]
pub enum McEvent {
    MessageReceived {
        msg: Message,
        src: String,
        dest: String,
        options: DeliveryOptions,
    },
    TimerFired {
        proc: String,
        timer: String,
    },
}

#[derive(Serialize, Clone)]
pub enum DeliveryOptions {
    /// Message will be received exactly once without corruption
    NoFailures,
    /// Message will not be received
    Dropped,
    /// Message delivery may be subject to some failures
    PossibleFailures {
        can_be_dropped: bool,
        max_dupl_count: u32,
        can_be_corrupted: bool,
    },
}
