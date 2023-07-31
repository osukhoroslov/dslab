//! Network model where the bandwidth is shared fairly among all current transfers.

use dslab_core::context::SimulationContext;
use dslab_models::throughput_sharing::{FairThroughputSharingModel, ThroughputSharingModel};

use crate::{DataTransfer, DataTransferCompleted, NetworkModel, NodeId};

/// Network model where the bandwidth is shared fairly among all current transfers.
pub struct SharedBandwidthNetworkModel {
    bandwidth: f64,
    latency: f64,
    throughput_model: FairThroughputSharingModel<DataTransfer>,
    next_event: u64,
}

impl SharedBandwidthNetworkModel {
    /// Creates a new network model with specified bandwidth and latency.
    pub fn new(bandwidth: f64, latency: f64) -> SharedBandwidthNetworkModel {
        SharedBandwidthNetworkModel {
            bandwidth,
            latency,
            throughput_model: FairThroughputSharingModel::with_fixed_throughput(bandwidth),
            next_event: 0,
        }
    }
}

impl NetworkModel for SharedBandwidthNetworkModel {
    fn is_topology_aware(&self) -> bool {
        false
    }

    fn bandwidth(&self, _src: NodeId, _dst: NodeId) -> f64 {
        self.bandwidth
    }

    fn latency(&self, _src: NodeId, _dst: NodeId) -> f64 {
        self.latency
    }

    fn start_transfer(&mut self, dt: DataTransfer, ctx: &mut SimulationContext) {
        ctx.cancel_event(self.next_event);
        let size = dt.size;
        self.throughput_model.insert(dt, size, ctx);
        if let Some((time, dt)) = self.throughput_model.peek() {
            self.next_event = ctx.emit_self(DataTransferCompleted { dt: dt.clone() }, time - ctx.time());
        }
    }

    fn on_transfer_completion(&mut self, _dt: DataTransfer, ctx: &mut SimulationContext) {
        self.throughput_model.pop().unwrap();
        if let Some((time, dt)) = self.throughput_model.peek() {
            self.next_event = ctx.emit_self(DataTransferCompleted { dt: dt.clone() }, time - ctx.time());
        }
    }

    fn on_topology_change(&mut self, _ctx: &mut SimulationContext) {}
}
