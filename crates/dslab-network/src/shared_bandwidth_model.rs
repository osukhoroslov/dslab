use dslab_core::component::Id;
use dslab_core::context::SimulationContext;

use dslab_models::throughput_sharing::{FairThroughputSharingModel, ThroughputSharingModel};

use crate::model::*;

pub struct SharedBandwidthNetwork {
    bandwidth: f64,
    latency: f64,
    throughput_model: FairThroughputSharingModel<Data>,
    next_event: u64,
}

impl SharedBandwidthNetwork {
    pub fn new(bandwidth: f64, latency: f64) -> SharedBandwidthNetwork {
        SharedBandwidthNetwork {
            bandwidth,
            latency,
            throughput_model: FairThroughputSharingModel::with_fixed_throughput(bandwidth),
            next_event: 0,
        }
    }
}

impl NetworkConfiguration for SharedBandwidthNetwork {
    fn latency(&self, _src: Id, _dest: Id) -> f64 {
        self.latency
    }

    fn bandwidth(&self, _src: Id, _dest: Id) -> f64 {
        self.bandwidth
    }
}

impl DataOperation for SharedBandwidthNetwork {
    fn send_data(&mut self, data: Data, ctx: &mut SimulationContext) {
        ctx.cancel_event(self.next_event);
        self.throughput_model.insert(ctx.time(), data.size, data);
        if let Some((time, data)) = self.throughput_model.peek() {
            self.next_event = ctx.emit_self(DataReceive { data: data.clone() }, time - ctx.time());
        }
    }

    fn receive_data(&mut self, _data: Data, ctx: &mut SimulationContext) {
        self.throughput_model.pop().unwrap();
        if let Some((time, data)) = self.throughput_model.peek() {
            self.next_event = ctx.emit_self(DataReceive { data: data.clone() }, time - ctx.time());
        }
    }

    fn recalculate_operations(&mut self, _ctx: &mut SimulationContext) {}
}

impl NetworkModel for SharedBandwidthNetwork {}
