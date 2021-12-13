use core::actor::{Actor, ActorContext, ActorId, Event};
use core::match_event;

#[derive(Debug)]
pub struct DataReadRequest {
    pub size: u64,
}

#[derive(Debug)]
pub struct DataWriteRequest {
    pub size: u64,
}

#[derive(Debug)]
pub struct DataReadCompleted {
    pub id: u64,
}

#[derive(Debug)]
pub struct DataWriteCompleted {
    pub id: u64,
}

pub struct Storage {
    id: ActorId,
    read_bandwidth: u64,
    write_bandwidth: u64,
    ready_time: f64,
}

impl Storage {
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

impl Actor for Storage {
    fn on(&mut self, event: Box<dyn Event>, from: ActorId, ctx: &mut ActorContext) {
        match_event!( event {
            DataReadRequest { size } => {
                println!("{} [{}] read request from {}: {:?}", ctx.time(), ctx.id, from, event);
                let read_time = *size as f64 / self.read_bandwidth as f64;
                self.ready_time = self.ready_time.max(ctx.time()) + read_time;
                ctx.emit(DataReadCompleted { id: ctx.event_id }, from, self.ready_time - ctx.time());
            },
            DataWriteRequest { size } => {
                println!("{} [{}] write request from {}: {:?}", ctx.time(), ctx.id, from, event);
                let write_time = *size as f64 / self.write_bandwidth as f64;
                self.ready_time = self.ready_time.max(ctx.time()) + write_time;
                ctx.emit(DataWriteCompleted { id: ctx.event_id }, from, self.ready_time - ctx.time());
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}
