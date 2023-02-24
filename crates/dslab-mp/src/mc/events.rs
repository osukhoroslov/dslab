use serde::Serialize;

use crate::message::Message;

#[derive(Serialize, Clone)]
pub enum McEvent {
    MessageReceived {
        msg: Message,
        src: String,
        dest: String,
        can_be_dropped: bool,
        max_dupl_count: u32,
        can_be_corrupted: bool,
    },
    TimerFired {
        proc: String,
        timer: String,
    },
}
