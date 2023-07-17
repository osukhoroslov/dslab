//! Describes network trait.

use serde::Serialize;

use dslab_core::component::Id;
use dslab_core::context::SimulationContext;

// NETWORK TYPES ///////////////////////////////////////////////////////////////////////////////////

/// Represents some data transferred over the network.
#[derive(Clone, Debug, Serialize)]
pub struct Data {
    /// Unique id.
    pub id: usize,
    /// Source of data.
    pub src: Id,
    /// Destination.
    pub dest: Id,
    /// Size of the data in MB.
    pub size: f64,
    /// Simulation component to notify when the data transfer completes.
    pub notification_dest: Id,
}

/// Represents message in the network.
#[derive(Clone, Serialize)]
pub struct Message {
    /// Unique id.
    pub id: usize,
    /// Source of data.
    pub src: Id,
    /// Destination.
    pub dest: Id,
    /// Contents of the message.
    pub data: String,
}

// EVENTS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Serialize)]
pub(crate) struct MessageSend {
    pub(crate) message: Message,
}

#[derive(Clone, Serialize)]
pub(crate) struct MessageReceive {
    pub(crate) message: Message,
}

#[derive(Clone, Serialize)]
pub(crate) struct DataTransferRequest {
    pub(crate) data: Data,
}

#[derive(Clone, Serialize)]
pub(crate) struct StartDataTransfer {
    pub(crate) data: Data,
}

#[derive(Clone, Serialize)]
pub(crate) struct DataReceive {
    pub(crate) data: Data,
}

/// Event describing delivered message.
#[derive(Clone, Serialize)]
pub struct MessageDelivery {
    /// Message.
    pub message: Message,
}

/// Event describing completed data transfer.
#[derive(Clone, Serialize)]
pub struct DataTransferCompleted {
    /// Data.
    pub data: Data,
}

// NETWORK MODEL TEMPLATE ///////////////////////////////////////////////////////////////////////////////////

/// Trait describing a struct which can operate with data.
pub trait DataOperation {
    /// Sends data.
    fn send_data(&mut self, data: Data, ctx: &mut SimulationContext);
    /// Callback for receiving data.
    fn receive_data(&mut self, data: Data, ctx: &mut SimulationContext);
    /// Recalculates all operations after any change in the list of ongoing transfers.
    fn recalculate_operations(&mut self, ctx: &mut SimulationContext);
}

/// Trait describing a struct which can provide latency and bandwidth between two simulation components.
pub trait NetworkConfiguration {
    /// Returns the latency between two simulation components.
    fn latency(&self, src: Id, dst: Id) -> f64;
    /// Returns the bandwidth between two simulation components.
    fn bandwidth(&self, src: Id, dst: Id) -> f64;
}

/// Trait describing a struct which can represent a network.
pub trait NetworkModel: DataOperation + NetworkConfiguration {}
