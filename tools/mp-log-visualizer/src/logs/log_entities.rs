use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum LogEntry {
    NodeStarted {
        time: f64,
        node: String,
        node_id: u32,
    },
    ProcessStarted {
        time: f64,
        node: String,
        proc: String,
    },
    LocalMessageSent {
        time: f64,
        msg_id: String,
        node: String,
        proc: String,
        msg: Message,
    },
    LocalMessageReceived {
        time: f64,
        msg_id: String,
        node: String,
        proc: String,
        msg: Message,
    },
    MessageSent {
        time: f64,
        msg_id: String,
        src_node: String,
        src_proc: String,
        dest_node: String,
        dest_proc: String,
        msg: Message,
    },
    MessageReceived {
        time: f64,
        msg_id: String,
    },
    MessageDropped {
        time: f64,
        msg_id: String,
    },
    NodeConnected {
        time: f64,
        node: String,
    },
    NodeDisconnected {
        time: f64,
        node: String,
    },
    NodeCrashed {
        time: f64,
        node: String,
    },
    NodeRecovered {
        time: f64,
        node: String,
    },
    TimerSet {
        time: f64,
        timer_id: String,
        timer_name: String,
        node: String,
        proc: String,
        delay: f64,
    },
    TimerFired {
        time: f64,
        timer_id: String,
    },
    TimerCancelled {
        time: f64,
        timer_id: String,
    },
    LinkDisabled {
        time: f64,
        from: String,
        to: String,
    },
    LinkEnabled {
        time: f64,
        from: String,
        to: String,
    },
    DropIncoming {
        time: f64,
        node: String,
    },
    PassIncoming {
        time: f64,
        node: String,
    },
    DropOutgoing {
        time: f64,
        node: String,
    },
    PassOutgoing {
        time: f64,
        node: String,
    },
    NetworkPartition {
        time: f64,
        group1: Vec<String>,
        group2: Vec<String>,
    },
    NetworkReset {
        time: f64,
    },
    ProcessStateUpdated {
        time: f64,
        node: String,
        proc: String,
        state: String,
    },
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Message {
    pub tip: String,
    pub data: String,
}
