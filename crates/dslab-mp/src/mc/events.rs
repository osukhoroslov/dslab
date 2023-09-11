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
        dst: String,

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
        dst: String,

        /// The id of original MessageReceived event.
        ///
        /// Can be `None` if the message is dropped unconditionally.
        receive_event_id: Option<McEventId>,
    },

    /// The event of duplicating a message. Created by model checking strategy.
    MessageDuplicated {
        /// The duplicated message itself.
        msg: Message,

        /// The name of the process that sent the message.
        src: String,

        /// The name of the process the message was sent to.
        dst: String,

        /// The id of original MessageReceived event.
        receive_event_id: McEventId,
    },

    /// The event of corrupting a message. Created by model checking strategy.
    MessageCorrupted {
        /// The original message.
        msg: Message,

        /// The message after corruption.
        corrupted_msg: Message,

        /// The name of the process that sent the message.
        src: String,

        /// The name of the process the message was sent to.
        dst: String,

        /// The id of original MessageReceived event.
        receive_event_id: McEventId,
    },
}

impl McEvent {
    /// Duplicates the MessageReceived event with decreased max_dupl_count.
    pub fn duplicate(&self) -> Option<McEvent> {
        match self {
            McEvent::MessageReceived {
                msg,
                src,
                dst,
                options:
                    DeliveryOptions::PossibleFailures {
                        can_be_dropped,
                        max_dupl_count,
                        can_be_corrupted,
                    },
            } => Some(McEvent::MessageReceived {
                msg: msg.clone(),
                src: src.clone(),
                dst: dst.clone(),
                options: DeliveryOptions::PossibleFailures {
                    can_be_dropped: *can_be_dropped,
                    max_dupl_count: max_dupl_count - 1,
                    can_be_corrupted: *can_be_corrupted,
                },
            }),
            _ => None,
        }
    }

    pub fn disable_duplications(&mut self) {
        if let McEvent::MessageReceived {
            options: DeliveryOptions::PossibleFailures { max_dupl_count, .. },
            ..
        } = self
        {
            *max_dupl_count = 0;
        }
    }

    pub fn to_log_entry(&self) -> LogEntry {
        match self {
            Self::MessageReceived {
                msg,
                src,
                dst,
                options: _,
            } => LogEntry::McMessageReceived {
                msg: msg.clone(),
                src: src.clone(),
                dst: dst.clone(),
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
            Self::MessageDropped { msg, src, dst, .. } => LogEntry::McMessageDropped {
                msg: msg.clone(),
                src: src.clone(),
                dst: dst.clone(),
            },
            Self::MessageDuplicated { msg, src, dst, .. } => LogEntry::McMessageDuplicated {
                msg: msg.clone(),
                src: src.clone(),
                dst: dst.clone(),
            },
            Self::MessageCorrupted {
                msg,
                corrupted_msg,
                src,
                dst,
                ..
            } => LogEntry::McMessageCorrupted {
                msg: msg.clone(),
                corrupted_msg: corrupted_msg.clone(),
                src: src.clone(),
                dst: dst.clone(),
            },
        }
    }
}
