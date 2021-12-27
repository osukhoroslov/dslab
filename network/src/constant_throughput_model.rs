use core::actor::ActorContext;

use crate::model::*;

pub struct ConstantThroughputNetwork {
    bandwidth: f64,
    latency: f64,
}

impl ConstantThroughputNetwork {
    pub fn new(bandwidth: f64, latency: f64) -> ConstantThroughputNetwork {
        return ConstantThroughputNetwork { bandwidth, latency };
    }
}

impl NetworkConfiguration for ConstantThroughputNetwork {
    fn latency(&self) -> f64 {
        self.latency
    }
}

impl DataOperation for ConstantThroughputNetwork {
    fn send_data(&mut self, data: Data, ctx: &mut ActorContext) {
        let new_message_delivery_time = data.size / self.bandwidth;
        ctx.emit(DataReceive { data }, ctx.id.clone(), new_message_delivery_time);
    }

    fn receive_data(&mut self, _data: Data, _ctx: &mut ActorContext) {}
}

impl NetworkModel for ConstantThroughputNetwork {}
