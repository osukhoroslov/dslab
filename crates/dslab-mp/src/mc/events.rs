//! Implementation of events used in model checking.

use serde::Serialize;

use crate::mc::network::DeliveryOptions;
use crate::mc::system::McTime;
use crate::message::Message;

/// Identifier of McEvent.
pub type McEventId = usize;

/// Special events used in model checking instead of standard events.
#[derive(Serialize, Clone, Eq, Hash, PartialEq, Debug)]
pub enum McEvent {
    /// The event of receiving non-local message by some process.
    MessageReceived {
        /// The message itself.
        msg: Message,

        /// The process where the message sent from.
        src: String,

        /// Destination process of the message.
        dest: String,

        /// Network delivery options for the message.
        options: DeliveryOptions,
    },

    /// The event of the timer expiration.
    TimerFired {
        /// The process to which the timer belongs to.
        proc: String,

        /// The timer name.
        timer: String,

        /// Timer duration.
        timer_delay: McTime,
    },

    /// The event of timer cancellation.
    TimerCancelled {
        /// The process to which the timer belongs to.
        proc: String,

        /// The timer name.
        timer: String,
    },

    /// The event of message drop. Created by model checking algorithm.
    MessageDropped {
        /// The dropped message itself.
        msg: Message,

        /// The process where the message sent from.
        src: String,

        /// Destination process of the message.
        dest: String,
    },
}

impl McEvent {
    /// Create the clone of self with decreased max_dupl_count.
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
