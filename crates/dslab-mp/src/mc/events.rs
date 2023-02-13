use serde::Serialize;

use crate::message::Message;

#[derive(Serialize, Clone)]
pub enum McEvent {
    MessageReceived { msg: Message, src: String, dest: String },
    TimerFired { proc: String, timer: String },
}

pub struct EventInfo {
    pub event: McEvent,
    pub can_be_dropped: bool,
    pub can_be_duplicated: bool,
    pub can_be_corrupted: bool,
}
