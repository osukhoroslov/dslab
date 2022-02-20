use core::actor::{Actor, ActorContext, ActorId, Event};
use core::match_event;
use core::sim::Simulation;

use crate::api::{DataReadCompleted, DataReadRequest, DataWriteCompleted, DataWriteRequest};

pub struct Disk {
    read_bandwidth: u64,
    write_bandwidth: u64,
    ready_time: f64,
    actor_name: String,
}

impl Disk {
    pub fn new(actor_id: &str, read_bandwidth: u64, write_bandwidth: u64) -> Self {
        Self {
            read_bandwidth,
            write_bandwidth,
            ready_time: 0.,
            actor_name: actor_id.to_string(),
        }
    }

    pub fn read_async(&self, size: u64, sim: &mut Simulation, actor_to_notify: ActorId) {
        sim.add_event_now(DataReadRequest { size }, actor_to_notify, ActorId::from(&self.actor_name));
    }

    pub fn write_async(&self, size: u64, sim: &mut Simulation, actor_to_notify: ActorId) {
        sim.add_event_now(DataWriteRequest { size }, actor_to_notify, ActorId::from(&self.actor_name));
    }
}

impl Actor for Disk {
    fn on(&mut self, event: Box<dyn Event>, from: ActorId, ctx: &mut ActorContext) {
        match_event!( event {
            DataReadRequest { size } => {
                println!("{} [{}] disk READ {} bytes request from {}", ctx.time(), ctx.id, size, from);
                let read_time = *size as f64 / self.read_bandwidth as f64;
                self.ready_time = self.ready_time.max(ctx.time()) + read_time;
                ctx.emit(DataReadCompleted { src_event_id: ctx.event_id, size: *size }, from, self.ready_time - ctx.time());
            },
            DataWriteRequest { size } => {
                println!("{} [{}] disk WRITE {} bytes request from {}", ctx.time(), ctx.id, size, from);
                let write_time = *size as f64 / self.write_bandwidth as f64;
                self.ready_time = self.ready_time.max(ctx.time()) + write_time;
                ctx.emit(DataWriteCompleted { src_event_id: ctx.event_id, size: *size }, from, self.ready_time - ctx.time());
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}
