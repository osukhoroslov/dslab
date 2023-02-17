use serde::Serialize;

use crate::message::Message;

#[derive(Serialize, Clone)]
pub enum McEvent {
    MessageReceived {
        msg: Message,
        src: String,
        dest: String,
        can_be_dropped: bool,
        can_be_duplicated: bool,
        can_be_corrupted: bool,
    },
    TimerFired {
        proc: String,
        timer: String,
    },
}
