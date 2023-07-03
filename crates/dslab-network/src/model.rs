use serde::Serialize;

use dslab_core::component::Id;
use dslab_core::context::SimulationContext;

// NETWORK TYPES ///////////////////////////////////////////////////////////////////////////////////

/// Represent some data which can be sent through the network.
#[derive(Clone, Debug, Serialize)]
pub struct Data {
    pub id: usize,
    pub src: Id,
    pub dest: Id,
    /// Size of the data in MB.
    pub size: f64,
    /// Actor to notify when the transfer completes.
    pub notification_dest: Id,
}

#[derive(Clone, Serialize)]
pub struct Message {
    pub id: usize,
    pub src: Id,
    pub dest: Id,
    pub data: String,
}

// EVENTS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Serialize)]
pub struct MessageSend {
    pub message: Message,
}

#[derive(Clone, Serialize)]
pub struct MessageReceive {
    pub message: Message,
}

#[derive(Clone, Serialize)]
pub struct MessageDelivery {
    pub message: Message,
}

#[derive(Clone, Serialize)]
pub struct DataTransferRequest {
    pub data: Data,
}

#[derive(Clone, Serialize)]
pub struct StartDataTransfer {
    pub data: Data,
}

#[derive(Clone, Serialize)]
pub struct DataReceive {
    pub data: Data,
}

#[derive(Clone, Serialize)]
pub struct DataTransferCompleted {
    pub data: Data,
}

// NETWORK MODEL TEMPLATE ///////////////////////////////////////////////////////////////////////////////////

pub trait DataOperation {
    fn send_data(&mut self, data: Data, ctx: &mut SimulationContext);
    fn receive_data(&mut self, data: Data, ctx: &mut SimulationContext);
    fn recalculate_operations(&mut self, ctx: &mut SimulationContext);
}

pub trait NetworkConfiguration {
    fn latency(&self, src: Id, dst: Id) -> f64;
    fn bandwidth(&self, src: Id, dst: Id) -> f64;
}

pub trait NetworkModel: DataOperation + NetworkConfiguration {}
