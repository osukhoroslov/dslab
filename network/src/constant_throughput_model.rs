use core::actor::{ActorContext};

use crate::model::*;

#[derive(Debug, Clone)]
pub struct ConstantThroughputNetwork {
    throughput: f64,
    min_delay: f64,
    log_level: LogLevel,
}

impl ConstantThroughputNetwork {
    pub fn new(throughput: f64) -> ConstantThroughputNetwork {
        return ConstantThroughputNetwork {
            throughput,
            min_delay: 0.,
            log_level: LogLevel::Empty,
        };
    }
}

impl DataOperation for ConstantThroughputNetwork {
    fn send_data(&mut self, data: Data, ctx: &mut ActorContext) {
        let new_message_delivery_time = data.size / self.throughput + self.min_delay;
        println!(
            "System time: {}, Data ID: {}, From: {}, To {}, Size: {}, Time: {}",
            ctx.time(),
            data.id,
            data.source,
            data.dest,
            data.size,
            new_message_delivery_time
        );
        ctx.emit(
            ReceiveData_ { data },
            ctx.id.clone(),
            new_message_delivery_time,
        );
    }

    fn receive_data(&mut self, data: Data, ctx: &mut ActorContext) {
        println!(
            "System time: {}, Data ID: {}, From: {}, To {}, Size: {}",
            ctx.time(),
            data.id,
            data.source,
            data.dest,
            data.size
        );
    }

    fn set_network_params(&mut self, min_delay: f64) {
        self.min_delay = min_delay;
    }
}

impl LogProperties for ConstantThroughputNetwork {
    fn set_log_level(&mut self, log_level: LogLevel) {
        self.log_level = log_level;
    }
}

impl NetworkModel for ConstantThroughputNetwork {}