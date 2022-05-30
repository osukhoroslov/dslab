use simcore::component::{Fractional, Id};
use simcore::context::SimulationContext;

use crate::model::*;

pub struct ConstantBandwidthNetwork {
    bandwidth: Fractional,
    latency: Fractional,
}

impl ConstantBandwidthNetwork {
    pub fn new(bandwidth: Fractional, latency: Fractional) -> ConstantBandwidthNetwork {
        return ConstantBandwidthNetwork { bandwidth, latency };
    }
}

impl NetworkConfiguration for ConstantBandwidthNetwork {
    fn latency(&self, _src: Id, _dest: Id) -> Fractional {
        self.latency
    }

    fn bandwidth(&self, _src: Id, _dest: Id) -> Fractional {
        self.bandwidth
    }
}

impl DataOperation for ConstantBandwidthNetwork {
    fn send_data(&mut self, data: Data, ctx: &mut SimulationContext) {
        let data_transfer_time = data.size / self.bandwidth;
        ctx.emit_self(DataReceive { data }, data_transfer_time);
    }

    fn receive_data(&mut self, _data: Data, _ctx: &mut SimulationContext) {}
}

impl NetworkModel for ConstantBandwidthNetwork {}
