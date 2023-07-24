//! Network model without congestion where each transfer gets the full bandwidth.

use dslab_core::context::SimulationContext;

use crate::{DataTransfer, DataTransferCompleted, NetworkModel, NodeId};

/// Network model without congestion where each transfer gets the full bandwidth.
pub struct ConstantBandwidthNetworkModel {
    bandwidth: f64,
    latency: f64,
}

impl ConstantBandwidthNetworkModel {
    /// Creates a new network model with specified bandwidth and latency.
    pub fn new(bandwidth: f64, latency: f64) -> ConstantBandwidthNetworkModel {
        ConstantBandwidthNetworkModel { bandwidth, latency }
    }
}

impl NetworkModel for ConstantBandwidthNetworkModel {
    fn is_topology_aware(&self) -> bool {
        false
    }

    fn bandwidth(&self, _src: NodeId, _dest: NodeId) -> f64 {
        self.bandwidth
    }

    fn latency(&self, _src: NodeId, _dest: NodeId) -> f64 {
        self.latency
    }

    fn start_transfer(&mut self, dt: DataTransfer, ctx: &mut SimulationContext) {
        let data_transfer_time = dt.size / self.bandwidth;
        ctx.emit_self(DataTransferCompleted { dt }, data_transfer_time);
    }

    fn on_transfer_completion(&mut self, _dt: DataTransfer, _ctx: &mut SimulationContext) {}

    fn on_topology_change(&mut self, _ctx: &mut SimulationContext) {}
}
