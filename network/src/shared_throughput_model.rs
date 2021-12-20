use std::collections::BTreeMap;
use log::{info, debug};

use core::actor::ActorContext;

use crate::model::*;

#[derive(Debug, Clone)]
struct DataTransfer {
    size_left: f64,
    last_speed: f64,
    last_time: f64,
    receive_event: u64,
    data: Data,
}

#[derive(Debug, Clone)]
pub struct SharedThroughputNetwork {
    throughput: f64,
    transfers: BTreeMap<usize, DataTransfer>
}

impl SharedThroughputNetwork {
    pub fn new(throughput: f64) -> SharedThroughputNetwork {
        return SharedThroughputNetwork {
            throughput,
            transfers: BTreeMap::new()
        };
    }

    fn recalculate_receive_time(&mut self, ctx: &mut ActorContext) {
        let cur_time = ctx.time();
        for (_, send_elem) in self.transfers.iter_mut() {
            let delivery_time = cur_time - send_elem.last_time;
            send_elem.size_left -= delivery_time * send_elem.last_speed;
            ctx.cancel_event(send_elem.receive_event);
        }

        let new_throughput = self.throughput / (self.transfers.len() as f64);

        for (_, send_elem) in self.transfers.iter_mut() {
            send_elem.last_speed = new_throughput;
            send_elem.last_time = cur_time;
            let data_delivery_time = send_elem.size_left / new_throughput;
            send_elem.receive_event = ctx.emit(
                DataReceive {
                    data: send_elem.data.clone(),
                },
                ctx.id.clone(),
                data_delivery_time,
            );
            debug!("System time: {}, Calculate. Data ID: {}, From: {}, To {}, Size: {}, SizeLeft: {}, New Time: {}",
                ctx.time(),
                send_elem.data.id,
                send_elem.data.source,
                send_elem.data.dest,
                send_elem.data.size,
                send_elem.size_left,
                data_delivery_time);
        }
    }
}

impl DataOperation for SharedThroughputNetwork {
    fn send_data(&mut self, data: Data, ctx: &mut ActorContext) {
        info!(
            "System time: {}, Send. Data ID: {}, From: {}, To {}, Size: {}",
            ctx.time(),
            data.id,
            data.source,
            data.dest,
            data.size.clone()
        );

        let new_send_data_progres = DataTransfer {
            size_left: data.size,
            last_speed: 0.,
            last_time: 0.,
            receive_event: 0,
            data: data,
        };

        let data_id = new_send_data_progres.data.id;
        if self.transfers.insert(data_id, new_send_data_progres).is_some() {
            panic!(
                "SharedThroughputNetwork: data with id {} already exist",
                data_id
            );
        }

        self.recalculate_receive_time(ctx);
    }

    fn receive_data(&mut self, data: Data, ctx: &mut ActorContext) {
        info!(
            "System time: {}, Received. Data ID: {}, From: {}, To {}, Size: {}",
            ctx.time(),
            data.id,
            data.source,
            data.dest,
            data.size
        );
        self.transfers.remove(&data.id);
        self.recalculate_receive_time(ctx);
    }
}

impl NetworkModel for SharedThroughputNetwork {}