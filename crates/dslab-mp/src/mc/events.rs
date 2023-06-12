//! Implementation of events used in model checking.

use serde::Serialize;

use crate::logger::LogEntry;
use crate::message::Message;

use crate::mc::network::DeliveryOptions;
use crate::mc::system::McTime;

/// Identifier of McEvent.
pub type McEventId = usize;

/// Special events used in model checking instead of standard events.
#[derive(Serialize, Clone, Eq, Hash, PartialEq, Debug)]
pub enum McEvent {
    /// The event of receiving a non-local message by a process.
    MessageReceived {
        /// The message itself.
        msg: Message,

        /// The name of the process that sent the message.
        src: String,

        /// The name of the process that received the message.
        dest: String,

        /// Network delivery options for the message.
        options: DeliveryOptions,
    },

    /// The event of a timer expiration.
    TimerFired {
        /// The process to which the timer belongs to.
        proc: String,

        /// The timer name.
        timer: String,

        /// The timer duration.
        timer_delay: McTime,
    },

    /// The event of cancelling a timer.
    TimerCancelled {
        /// The process to which the timer belongs to.
        proc: String,

        /// The timer name.
        timer: String,
    },

    /// The event of dropping a message. Created by the model checking strategy.
    MessageDropped {
        /// The dropped message itself.
        msg: Message,

        /// The name of the process that sent the message.
        src: String,

        /// The name of the process the message was sent to.
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

impl McEvent {
    pub fn to_log_entry(&self) -> LogEntry {
        match self {
            Self::MessageReceived {
                msg,
                src,
                dest,
                options: _,
            } => LogEntry::McMessageReceived {
                msg: msg.clone(),
                src: src.clone(),
                dest: dest.clone(),
            },
            Self::TimerFired {
                proc,
                timer,
                timer_delay: _,
            } => LogEntry::McTimerFired {
                proc: proc.clone(),
                timer: timer.clone(),
            },
            Self::TimerCancelled { proc, timer } => LogEntry::McTimerCancelled {
                proc: proc.clone(),
                timer: timer.clone(),
            },
            Self::MessageDropped { msg, src, dest } => LogEntry::McMessageDropped {
                msg: msg.clone(),
                src: src.clone(),
                dest: dest.clone(),
            },
        }
    }
}
