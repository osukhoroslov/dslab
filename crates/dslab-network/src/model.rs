//! Network model interface.

use serde::Serialize;
use std::cell::RefCell;
use std::rc::Rc;

use dslab_core::component::Id;
use dslab_core::context::SimulationContext;

use crate::routing::RoutingAlgorithm;
use crate::{NodeId, Topology};

/// Represents a data transfer between two simulation components located on a network.
#[derive(Clone, Debug, Serialize)]
pub struct DataTransfer {
    /// Unique transfer id.
    pub id: usize,
    /// Simulation component which is sending the data.
    pub src: Id,
    /// Node id of data sender.
    pub src_node_id: NodeId,
    /// Simulation component which is receiving the data.
    pub dest: Id,
    /// Node id of data receiver.
    pub dest_node_id: NodeId,
    /// Data size.
    pub size: f64,
    /// Simulation component to notify when the transfer is completed.
    pub notification_dest: Id,
}

/// Event signalling the completion of data transfer.
#[derive(Clone, Serialize)]
pub struct DataTransferCompleted {
    /// Completed data transfer.
    pub dt: DataTransfer,
}

/// Network model interface.
///
/// The main functions of the network model:
/// - Provide bandwidth and latency between network nodes,
/// - Calculate data transfer completion times and emit [`DataTransferCompleted`] events.
///
/// A topology-aware model uses information about the network topology (links connecting the nodes)
/// and relies on a routing algorithm to compute paths between the nodes.
pub trait NetworkModel {
    /// Returns true is the model is topology-aware.
    fn is_topology_aware(&self) -> bool;

    /// Performs initialization of topology-aware model.
    ///
    /// This method is used for passing network topology and routing algorithm.
    fn init(&mut self, _topology: Rc<RefCell<Topology>>, _routing: Box<dyn RoutingAlgorithm>) {}

    /// Returns the network bandwidth from node `src` to node `dest`.
    fn bandwidth(&self, src: NodeId, dest: NodeId) -> f64;

    /// Returns the network latency from node `src` to node `dest`.
    fn latency(&self, src: NodeId, dest: NodeId) -> f64;

    /// Starts data transfer.
    ///
    /// Must calculate the transfer completion time and emit the [`DataTransferCompleted`] event at this time.
    /// The event must be emitted via the passed simulation context using [`SimulationContext::emit_self`].
    fn start_transfer(&mut self, dt: DataTransfer, ctx: &mut SimulationContext);

    /// Callback for notifying the model about data transfer completion.
    ///
    /// This is necessary since the model itself does not receive the [`DataTransferCompleted`] event.
    fn on_transfer_completion(&mut self, dt: DataTransfer, ctx: &mut SimulationContext);

    /// Callback for notifying topology-aware model about the topology change.
    ///
    /// This is necessary since the topology change may require recalculation of transfer completion times.
    fn on_topology_change(&mut self, ctx: &mut SimulationContext);
}
