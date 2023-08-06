//! Network model interface.

use serde::Serialize;

use dslab_core::component::Id;
use dslab_core::context::SimulationContext;

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
    pub dst: Id,
    /// Node id of data receiver.
    pub dst_node_id: NodeId,
    /// Data size.
    pub size: f64,
    /// Simulation component to notify when the transfer is completed.
    pub notification_dst: Id,
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

    /// Returns the network bandwidth from node `src` to node `dst`.
    fn bandwidth(&self, src: NodeId, dst: NodeId) -> f64;

    /// Returns the network latency from node `src` to node `dst`.
    fn latency(&self, src: NodeId, dst: NodeId) -> f64;

    /// Starts data transfer.
    ///
    /// Must calculate the transfer completion time and emit the [`DataTransferCompleted`] event at this time.
    /// The event must be emitted via the passed simulation context using [`SimulationContext::emit_self`].
    fn start_transfer(&mut self, dt: DataTransfer, ctx: &mut SimulationContext);

    /// Callback for notifying the model about data transfer completion.
    ///
    /// This is necessary since the model itself does not receive the [`DataTransferCompleted`] event.
    fn on_transfer_completion(&mut self, dt: DataTransfer, ctx: &mut SimulationContext);

    /// Returns a reference to inner network topology.
    ///
    /// Must be implemented for topology-aware model.
    fn topology(&self) -> Option<&Topology> {
        assert!(
            !self.is_topology_aware(),
            "This method must be implemented for topology-aware model"
        );
        None
    }

    /// Returns a mutable reference to inner network topology.
    ///
    /// Must be implemented for topology-aware model.
    fn topology_mut(&mut self) -> Option<&mut Topology> {
        assert!(
            !self.is_topology_aware(),
            "This method must be implemented for topology-aware model"
        );
        None
    }

    /// Callback for notifying topology-aware model about the topology change.
    ///
    /// Must be implemented for topology-aware model.
    fn on_topology_change(&mut self, _ctx: &mut SimulationContext) {
        assert!(
            !self.is_topology_aware(),
            "This method must be implemented for topology-aware model"
        );
    }
}
