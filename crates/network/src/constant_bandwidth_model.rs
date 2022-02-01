use core::actor::ActorContext;

use crate::model::*;

pub struct ConstantBandwidthNetwork {
    bandwidth: f64,
    latency: f64,
}

impl ConstantBandwidthNetwork {
    pub fn new(bandwidth: f64, latency: f64) -> ConstantBandwidthNetwork {
        return ConstantBandwidthNetwork { bandwidth, latency };
    }
}

impl NetworkConfiguration for ConstantBandwidthNetwork {
    fn latency(&self) -> f64 {
        self.latency
    }
}

impl DataOperation for ConstantBandwidthNetwork {
    fn send_data(&mut self, data: Data, ctx: &mut ActorContext) {
        let data_transfer_time = data.size / self.bandwidth;
        ctx.emit(DataReceive { data }, ctx.id.clone(), data_transfer_time);
    }

    fn receive_data(&mut self, _data: Data, _ctx: &mut ActorContext) {}
}

impl NetworkModel for ConstantBandwidthNetwork {}
