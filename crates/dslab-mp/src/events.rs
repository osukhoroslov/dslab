//! Simulation events.

use serde::Serialize;

use crate::message::Message;

/// Message is received.
#[derive(Clone, Serialize)]
pub struct MessageReceived {
    /// Message identifier.
    pub id: u64,
    /// Received message.
    pub msg: Message,
    /// Name of sender process.
    pub src: String,
    /// Name of sender node.
    pub src_node: String,
    /// Name of destination process.
    pub dst: String,
    /// Name of destination node.
    pub dst_node: String,
}

/// Timer is fired.
#[derive(Clone, Serialize)]
pub struct TimerFired {
    /// Name of process that set the timer.
    pub proc: String,
    /// Timer name.
    pub timer: String,
}
