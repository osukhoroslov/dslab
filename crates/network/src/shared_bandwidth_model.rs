use simcore::component::{Fractional, Id};
use simcore::context::SimulationContext;

use throughput_model::fair_sharing_slow::FairThroughputSharingSlowModel;
use throughput_model::model::ThroughputModel;

use crate::model::*;

pub struct SharedBandwidthNetwork {
    bandwidth: Fractional,
    latency: Fractional,
    throughput_model: FairThroughputSharingSlowModel<Data>,
    next_event: u64,
}

impl SharedBandwidthNetwork {
    pub fn new(bandwidth: Fractional, latency: Fractional) -> SharedBandwidthNetwork {
        return SharedBandwidthNetwork {
            bandwidth,
            latency,
            throughput_model: FairThroughputSharingSlowModel::with_fixed_throughput(bandwidth),
            next_event: 0,
        };
    }
}

impl NetworkConfiguration for SharedBandwidthNetwork {
    fn latency(&self, _src: Id, _dest: Id) -> Fractional {
        self.latency
    }

    fn bandwidth(&self, _src: Id, _dest: Id) -> Fractional {
        self.bandwidth
    }
}

impl DataOperation for SharedBandwidthNetwork {
    fn send_data(&mut self, data: Data, ctx: &mut SimulationContext) {
        ctx.cancel_event(self.next_event);
        self.throughput_model.insert(ctx.time(), data.size, data.clone());
        if let Some((time, data)) = self.throughput_model.next_time() {
            self.next_event = ctx.emit_self(DataReceive { data: data.clone() }, time - ctx.time());
        }
    }

    fn receive_data(&mut self, _data: Data, ctx: &mut SimulationContext) {
        self.throughput_model.pop().unwrap();
        if let Some((time, data)) = self.throughput_model.next_time() {
            self.next_event = ctx.emit_self(DataReceive { data: data.clone() }, time - ctx.time());
        }
    }
}

impl NetworkModel for SharedBandwidthNetwork {}
