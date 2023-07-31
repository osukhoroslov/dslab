use serde::Serialize;

use crate::message::Message;

#[derive(Clone, Serialize)]
pub struct MessageReceived {
    pub id: u64,
    pub msg: Message,
    pub src: String,
    pub src_node: String,
    pub dst: String,
    pub dst_node: String,
}

#[derive(Clone, Serialize)]
pub struct TimerFired {
    pub proc: String,
    pub timer: String,
}
