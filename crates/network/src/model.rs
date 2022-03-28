use serde::Serialize;

use core::context::SimulationContext;

// NETWORK TYPES ///////////////////////////////////////////////////////////////////////////////////

#[derive(Serialize, Debug, Clone)]
pub struct Data {
    pub id: usize,
    pub src: u32,
    pub dest: u32,
    pub size: f64,
    pub notification_dest: u32,
}

#[derive(Serialize, Clone)]
pub struct Message {
    pub id: usize,
    pub src: u32,
    pub dest: u32,
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
    fn latency(&self, src: u32, dst: u32) -> f64;
}

pub trait NetworkModel: DataOperation + NetworkConfiguration {}
