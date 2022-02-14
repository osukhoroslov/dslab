use core::actor::{Actor, ActorContext, ActorId, Event};
use core::match_event;

use crate::api::{DataReadCompleted, DataReadRequest, DataWriteCompleted, DataWriteRequest};

pub struct Disk {
    id: ActorId,
    read_bandwidth: u64,
    write_bandwidth: u64,
    ready_time: f64,
}

impl Disk {
    pub fn new(id: &str, read_bandwidth: u64, write_bandwidth: u64) -> Self {
        Self {
            id: ActorId::from(id),
            read_bandwidth,
            write_bandwidth,
            ready_time: 0.,
        }
    }

    pub fn read(&self, size: u64, ctx: &mut ActorContext) -> u64 {
        let req = DataReadRequest { size };
        ctx.emit_now(req, self.id.clone())
    }

    pub fn write(&self, size: u64, ctx: &mut ActorContext) -> u64 {
        let req = DataWriteRequest { size };
        ctx.emit_now(req, self.id.clone())
    }
}

impl Actor for Disk {
    fn on(&mut self, event: Box<dyn Event>, from: ActorId, ctx: &mut ActorContext) {
        match_event!( event {
            DataReadRequest { size } => {
                println!("{} [{}] read request from {}: {:?}", ctx.time(), ctx.id, from, event);
                let read_time = *size as f64 / self.read_bandwidth as f64;
                self.ready_time = self.ready_time.max(ctx.time()) + read_time;
                ctx.emit(DataReadCompleted { src_event_id: ctx.event_id }, from, self.ready_time - ctx.time());
            },
            DataWriteRequest { size } => {
                println!("{} [{}] write request from {}: {:?}", ctx.time(), ctx.id, from, event);
                let write_time = *size as f64 / self.write_bandwidth as f64;
                self.ready_time = self.ready_time.max(ctx.time()) + write_time;
                ctx.emit(DataWriteCompleted { src_event_id: ctx.event_id }, from, self.ready_time - ctx.time());
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}
