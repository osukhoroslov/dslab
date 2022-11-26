use dslab_core::component::Id;
use dslab_core::context::SimulationContext;

use crate::model::*;

pub struct ConstantBandwidthNetwork {
    bandwidth: f64,
    latency: f64,
}

impl ConstantBandwidthNetwork {
    pub fn new(bandwidth: f64, latency: f64) -> ConstantBandwidthNetwork {
        ConstantBandwidthNetwork { bandwidth, latency }
    }
}

impl NetworkConfiguration for ConstantBandwidthNetwork {
    fn latency(&self, _src: Id, _dest: Id) -> f64 {
        self.latency
    }

    fn bandwidth(&self, _src: Id, _dest: Id) -> f64 {
        self.bandwidth
    }
}

impl DataOperation for ConstantBandwidthNetwork {
    fn send_data(&mut self, data: Data, ctx: &mut SimulationContext) {
        let data_transfer_time = data.size / self.bandwidth;
        ctx.emit_self(DataReceive { data }, data_transfer_time);
    }

    fn receive_data(&mut self, _data: Data, _ctx: &mut SimulationContext) {}
    fn recalculate_operations(&mut self, _ctx: &mut SimulationContext) {}
}

impl NetworkModel for ConstantBandwidthNetwork {}
