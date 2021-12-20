use core::actor::ActorContext;
use log::info;

use crate::model::*;

#[derive(Debug, Clone)]
pub struct ConstantThroughputNetwork {
    throughput: f64
}

impl ConstantThroughputNetwork {
    pub fn new(throughput: f64) -> ConstantThroughputNetwork {
        return ConstantThroughputNetwork {
            throughput
        };
    }
}

impl DataOperation for ConstantThroughputNetwork {
    fn send_data(&mut self, data: Data, ctx: &mut ActorContext) {
        let new_message_delivery_time = data.size / self.throughput;
        info!(
            "System time: {}, Data ID: {}, From: {}, To {}, Size: {}, Time: {}",
            ctx.time(),
            data.id,
            data.source,
            data.dest,
            data.size,
            new_message_delivery_time
        );
        ctx.emit(
            DataReceive { data },
            ctx.id.clone(),
            new_message_delivery_time,
        );
    }

    fn receive_data(&mut self, data: Data, ctx: &mut ActorContext) {
        info!(
            "System time: {}, Data ID: {}, From: {}, To {}, Size: {}",
            ctx.time(),
            data.id,
            data.source,
            data.dest,
            data.size
        );
    }
}

impl NetworkModel for ConstantThroughputNetwork {}