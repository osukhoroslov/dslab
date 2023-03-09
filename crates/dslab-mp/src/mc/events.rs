use serde::Serialize;

use crate::message::Message;

#[derive(Serialize, Clone, Eq, PartialEq, Hash)]
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

#[derive(Serialize, Clone, Eq, Hash, PartialEq)]
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
    TimerCancelled {
        proc: String,
        timer: String,
    },
}

impl McEvent {
    pub fn duplicate(&self) -> Option<McEvent> {
        match self {
            McEvent::MessageReceived {
                msg,
                src,
                dest,
                options:
                    DeliveryOptions::PossibleFailures {
                        can_be_dropped,
                        max_dupl_count,
                        can_be_corrupted,
                    },
            } => Some(McEvent::MessageReceived {
                msg: msg.clone(),
                src: src.clone(),
                dest: dest.clone(),
                options: DeliveryOptions::PossibleFailures {
                    can_be_dropped: *can_be_dropped,
                    max_dupl_count: max_dupl_count - 1,
                    can_be_corrupted: *can_be_corrupted,
                },
            }),
            _ => None,
        }
    }
}
