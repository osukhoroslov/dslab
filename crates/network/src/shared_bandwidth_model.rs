use simcore::component::Id;
use simcore::context::SimulationContext;

use crate::model::*;
use crate::throughput_model::ThroughputModel;

pub struct SharedBandwidthNetwork {
    bandwidth: f64,
    latency: f64,
    throughput_model: ThroughputModel<Data>,
    next_event: u64,
}

impl SharedBandwidthNetwork {
    pub fn new(bandwidth: f64, latency: f64) -> SharedBandwidthNetwork {
        return SharedBandwidthNetwork {
            bandwidth,
            latency,
            throughput_model: ThroughputModel::new(bandwidth),
            next_event: 0,
        };
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
        self.throughput_model.insert(ctx.time(), data.size, data.clone());
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
}

impl NetworkModel for SharedBandwidthNetwork {}
