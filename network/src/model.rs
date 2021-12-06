use core::actor::{ActorContext, ActorId};

// Logging ///////////////////////////////////////////////////////////////////////////////////
#[derive(Debug, Clone)]
pub enum LogLevel {
    Empty = 0,
    SendReceive = 1,
    Full = 2,
}

pub fn check_log_level(log_level: LogLevel, expected_log_level: LogLevel) -> bool {
    return (log_level as usize) & (expected_log_level as usize) != 0;
}

// NETWORK TYPES ///////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct Data {
    pub id: usize,
    pub source: ActorId,
    pub dest: ActorId,
    pub size: f64,
}

#[derive(Debug, Clone)]
pub struct Message {
    pub id: usize,
    pub source: ActorId,
    pub dest: ActorId,
    pub data: String,
}

// EVENTS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct SendMessage {
    pub message: Message,
}

#[derive(Debug)]
pub struct ReceiveMessage_ {
    pub message: Message,
}

#[derive(Debug)]
pub struct ReceiveMessage {
    pub message: Message,
}

#[derive(Debug)]
pub struct SendData {
    pub data: Data,
}

#[derive(Debug)]
pub struct ReceiveData_ {
    pub data: Data,
}

#[derive(Debug)]
pub struct ReceiveData {
    pub data: Data,
}

// NETWORK MODEL TEMPLATE ///////////////////////////////////////////////////////////////////////////////////

pub trait DataOperation {
    fn send_data(&mut self, data: Data, ctx: &mut ActorContext);
    fn receive_data(&mut self, data: Data, ctx: &mut ActorContext);
    fn set_network_params(&mut self, min_delay: f64);
}

pub trait LogProperties {
    fn set_log_level(&mut self, log_level: LogLevel);
}

pub trait NetworkModel: DataOperation + LogProperties {}