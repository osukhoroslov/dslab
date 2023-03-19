use std::ops::Add;

use ordered_float::OrderedFloat;
use serde::Serialize;

use crate::message::Message;

#[derive(Serialize, Clone, PartialEq, Eq, Hash)]
pub enum DeliveryOptions {
    /// Message will be received exactly once without corruption with specified max delay
    NoFailures(SystemTime),
    /// Message will not be received
    Dropped,
    /// Message delivery may be subject to some failures
    PossibleFailures {
        can_be_dropped: bool,
        max_dupl_count: u32,
        can_be_corrupted: bool,
    },
}

#[derive(Debug, PartialEq, Eq, Clone, Hash, Ord, Copy, PartialOrd, Default)]
pub struct SystemTime(pub OrderedFloat<f64>);

impl Serialize for SystemTime {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer {
        serializer.serialize_f64(self.0.0)
    }
}

impl From<f64> for SystemTime {
    fn from(value: f64) -> Self {
        Self(OrderedFloat(value))
    }
}

impl Add for SystemTime {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self(self.0 + rhs.0)
    }
}

pub type McEventId = usize;

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
        duration: SystemTime,
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
