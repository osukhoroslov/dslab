use serde::Serialize;

use crate::message::Message;

#[derive(Clone, Serialize)]
pub struct MessageReceived {
    pub msg: Message,
    pub src: String,
    pub dest: String,
}

#[derive(Clone, Serialize)]
pub struct TimerFired {
    pub proc: String,
    pub timer: String,
}
