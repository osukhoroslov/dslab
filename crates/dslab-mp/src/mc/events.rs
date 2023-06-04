use ordered_float::OrderedFloat;
use serde::Serialize;

use crate::mc::network::DeliveryOptions;
use crate::message::Message;

pub type McTime = OrderedFloat<f64>;
pub type McEventId = usize;

#[derive(Serialize, Clone, Eq, Hash, PartialEq, Debug)]
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
        timer_delay: McTime,
    },
    TimerCancelled {
        proc: String,
        timer: String,
    },
    MessageDropped {
        msg: Message,
        src: String,
        dest: String,
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
