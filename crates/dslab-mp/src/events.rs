use serde::Serialize;

use crate::message::Message;

#[derive(Serialize)]
pub struct MessageSent {
    pub msg: Message,
    pub src: String,
    pub dest: String,
}

#[derive(Serialize, Clone)]
pub struct MessageReceived {
    pub msg: Message,
    pub src: String,
    pub dest: String,
}

#[derive(Serialize)]
pub struct LocalMessageReceived {
    pub msg: Message,
    pub dest: String,
}

#[derive(Serialize)]
pub struct TimerSet {
    pub timer_name: String,
    pub proc_name: String,
    pub delay: f64,
}

#[derive(Serialize)]
pub struct TimerFired {
    pub timer_name: String,
    pub proc_name: String,
}
