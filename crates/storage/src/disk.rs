use core::actor::{Actor, ActorContext, ActorId, Event};
use core::match_event;

use crate::api::{DataReadCompleted, DataReadRequest, DataWriteCompleted, DataWriteRequest};

#[derive(Debug)]
pub struct Disk {
    name: String,
    capacity: u64,
    used: u64,
    read_bandwidth: u64,
    write_bandwidth: u64,
    ready_time: f64,
}

impl Disk {
    pub fn new(name: &str, capacity: u64, read_bandwidth: u64, write_bandwidth: u64) -> Self {
        Self {
            name: name.to_string(),
            capacity,
            used: 0,
            read_bandwidth,
            write_bandwidth,
            ready_time: 0.,
        }
    }

    pub fn read(&self, size: u64, ctx: &mut ActorContext) -> u64 {
        ctx.emit_now(DataReadRequest { size }, ActorId::from(&self.name))
    }

    pub fn write(&self, size: u64, ctx: &mut ActorContext) -> u64 {
        ctx.emit_now(DataWriteRequest { size }, ActorId::from(&self.name))
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

                let remaining = self.capacity - self.used;
                let size_to_write = if remaining < *size {
                    remaining
                } else {
                    *size
                };
                self.used += size_to_write;

                let write_time = size_to_write as f64 / self.write_bandwidth as f64;
                self.ready_time = self.ready_time.max(ctx.time()) + write_time;

                ctx.emit(DataWriteCompleted { src_event_id: ctx.event_id, size: size_to_write }, from, self.ready_time - ctx.time());
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}
