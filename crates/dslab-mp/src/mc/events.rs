use serde::Serialize;

use crate::message::Message;

#[derive(Serialize, Clone)]
pub enum McEvent {
    MessageReceived { msg: Message, src: String, dest: String },
    TimerFired { proc: String, timer: String },
}
