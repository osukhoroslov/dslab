use serde::Serialize;

use core::context::SimulationContext;

// NETWORK TYPES ///////////////////////////////////////////////////////////////////////////////////

#[derive(Serialize, Debug, Clone)]
pub struct Data {
    pub id: usize,
    pub src: String,
    pub dest: String,
    pub size: f64,
    pub notification_dest: String,
}

#[derive(Serialize, Clone)]
pub struct Message {
    pub id: usize,
    pub src: String,
    pub dest: String,
    pub data: String,
}

// EVENTS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Serialize)]
pub struct MessageSend {
    pub message: Message,
}

#[derive(Serialize)]
pub struct MessageReceive {
    pub message: Message,
}

#[derive(Serialize)]
pub struct MessageDelivery {
    pub message: Message,
}

#[derive(Serialize)]
pub struct DataTransferRequest {
    pub data: Data,
}

#[derive(Serialize)]
pub struct StartDataTransfer {
    pub data: Data,
}

#[derive(Serialize)]
pub struct DataReceive {
    pub data: Data,
}

#[derive(Serialize)]
pub struct DataTransferCompleted {
    pub data: Data,
}

// NETWORK MODEL TEMPLATE ///////////////////////////////////////////////////////////////////////////////////

pub trait DataOperation {
    fn send_data(&mut self, data: Data, ctx: &mut SimulationContext);
    fn receive_data(&mut self, data: Data, ctx: &mut SimulationContext);
}

pub trait NetworkConfiguration {
    fn latency(&self, src: &str, dst: &str) -> f64;
}

pub trait NetworkModel: DataOperation + NetworkConfiguration {}
