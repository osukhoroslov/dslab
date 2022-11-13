use serde::Serialize;

use crate::message::Message;

#[derive(Serialize, Clone)]
pub struct MessageReceived {
    pub msg: Message,
    pub src: String,
    pub dest: String,
}

#[derive(Serialize)]
pub struct TimerFired {
    pub proc: String,
    pub timer: String,
}
