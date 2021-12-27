use core::actor::{ActorContext, ActorId, Event};

// NETWORK TYPES ///////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct Data {
    pub id: usize,
    pub source: ActorId,
    pub dest: ActorId,
    pub size: f64,
    pub notification_dest: ActorId,
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
pub struct EventLatency {
    pub event: Box<dyn Event>,
}

#[derive(Debug)]
pub struct MessageSend {
    pub message: Message,
}

#[derive(Debug)]
pub struct MessageReceive {
    pub message: Message,
}

#[derive(Debug)]
pub struct MessageDelivery {
    pub message: Message,
}

#[derive(Debug)]
pub struct DataTransferRequest {
    pub data: Data,
}

#[derive(Debug)]
pub struct StartDataTransfer {
    pub data: Data,
}

#[derive(Debug)]
pub struct DataReceive {
    pub data: Data,
}

#[derive(Debug)]
pub struct DataTransferCompleted {
    pub data: Data,
}

// NETWORK MODEL TEMPLATE ///////////////////////////////////////////////////////////////////////////////////

pub trait DataOperation {
    fn send_data(&mut self, data: Data, ctx: &mut ActorContext);
    fn receive_data(&mut self, data: Data, ctx: &mut ActorContext);
}

pub trait NetworkConfiguration {
    fn latency(&self) -> f64;
}

pub trait NetworkModel: DataOperation + NetworkConfiguration {}
