use std::collections::BTreeMap;

use core::actor::{ActorContext};

use crate::model::*;

#[derive(Debug, Clone)]
struct SendDataProgress {
    delay_left: f64,
    size_left: f64,
    last_speed: f64,
    last_time: f64,
    receive_event: u64,
    data: Data,
}

#[derive(Debug, Clone)]
pub struct SharedThroughputNetwork {
    throughput: f64,
    cur: BTreeMap<usize, SendDataProgress>,
    min_delay: f64,
    log_level: LogLevel,
}

impl SharedThroughputNetwork {
    pub fn new(throughput: f64) -> SharedThroughputNetwork {
        return SharedThroughputNetwork {
            throughput,
            cur: BTreeMap::new(),
            min_delay: 0.,
            log_level: LogLevel::Empty,
        };
    }

    fn recalculate_receive_time(&mut self, ctx: &mut ActorContext) {
        let cur_time = ctx.time();
        for (_, send_elem) in self.cur.iter_mut() {
            let mut delivery_time = cur_time - send_elem.last_time;
            if delivery_time > send_elem.delay_left {
                delivery_time -= send_elem.delay_left;
                send_elem.delay_left = 0.0;
            } else {
                send_elem.delay_left -= delivery_time;
                delivery_time = 0.0;
            }
            send_elem.size_left -= delivery_time * send_elem.last_speed;
            ctx.cancel_event(send_elem.receive_event);
        }

        let new_throughput = self.throughput / (self.cur.len() as f64);

        for (_, send_elem) in self.cur.iter_mut() {
            send_elem.last_speed = new_throughput;
            send_elem.last_time = cur_time;
            let data_delivery_time = send_elem.size_left / new_throughput + send_elem.delay_left;
            send_elem.receive_event = ctx.emit(
                ReceiveData_ {
                    data: send_elem.data.clone(),
                },
                ctx.id.clone(),
                data_delivery_time,
            );
            if check_log_level(self.log_level.clone(), LogLevel::Full) {
                println!("System time: {}, Calculate. Data ID: {}, From: {}, To {}, Size: {}, SizeLeft: {}, New Time: {}",
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
}

impl DataOperation for SharedThroughputNetwork {
    fn send_data(&mut self, data: Data, ctx: &mut ActorContext) {
        if check_log_level(self.log_level.clone(), LogLevel::SendReceive) {
            println!(
                "System time: {}, Send. Data ID: {}, From: {}, To {}, Size: {}",
                ctx.time(),
                data.id,
                data.source,
                data.dest,
                data.size.clone()
            );
        }

        let new_send_data_progres = SendDataProgress {
            delay_left: self.min_delay,
            size_left: data.size,
            last_speed: 0.,
            last_time: 0.,
            receive_event: 0,
            data: data,
        };

        let data_id = new_send_data_progres.data.id;
        if self.cur.insert(data_id, new_send_data_progres).is_some() {
            panic!(
                "SharedThroughputNetwork: data with id {} already exist",
                data_id
            );
        }

        self.recalculate_receive_time(ctx);
    }

    fn receive_data(&mut self, data: Data, ctx: &mut ActorContext) {
        if check_log_level(self.log_level.clone(), LogLevel::SendReceive) {
            println!(
                "System time: {}, Received. Data ID: {}, From: {}, To {}, Size: {}",
                ctx.time(),
                data.id,
                data.source,
                data.dest,
                data.size
            );
        }
        self.cur.remove(&data.id);
        self.recalculate_receive_time(ctx);
    }

    fn set_network_params(&mut self, min_delay: f64) {
        self.min_delay = min_delay;
    }
}

impl LogProperties for SharedThroughputNetwork {
    fn set_log_level(&mut self, log_level: LogLevel) {
        self.log_level = log_level;
    }
}

impl NetworkModel for SharedThroughputNetwork {}