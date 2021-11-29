use core::match_event;
use core::actor::{Actor, ActorId, ActorContext, Event};

// EVENTS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct DiskReadRequest {
    pub size: u64
}

#[derive(Debug)]
pub struct DiskWriteRequest {
    pub size: u64
}

#[derive(Debug)]
pub struct DiskReadCompleted {
    pub src_event_id: u64
}

#[derive(Debug)]
pub struct DiskWriteCompleted {
    pub src_event_id: u64
}

// ACTORS //////////////////////////////////////////////////////////////////////////////////////////

pub struct Disk {
    read_bandwidth: u64,
    write_bandwidth: u64,
    release_time: f64
}

impl Disk {
    pub fn new(read_bandwidth: u64, write_bandwidth: u64, current_time: f64) -> Self {
        Self {read_bandwidth, write_bandwidth, release_time: current_time}
    }

    pub fn get_read_bandwidth(&self) -> u64 {
        self.read_bandwidth
    }

    pub fn get_write_bandwidth(&self) -> u64 {
        self.write_bandwidth
    }

    fn calc_read_delay(&mut self, size : u64) -> f64 {
        let delay = size as f64 / self.read_bandwidth as f64;
        self.release_time += delay;
        delay
    }

    fn calc_write_delay(&mut self, size : u64) -> f64 {
        let delay = size as f64 / self.write_bandwidth as f64;
        self.release_time += delay;
        delay
    }
}

impl Actor for Disk {
    fn on(&mut self, event: Box<dyn Event>, from: ActorId, ctx: &mut ActorContext) {
        match_event!( event {
            DiskReadRequest { size } => {
                println!("{} [{}] received DiskReadRequest from {}", ctx.time(), ctx.id, from);
                ctx.emit(DiskReadCompleted { src_event_id: ctx.event_id }, from, self.calc_read_delay(*size));
            },
            DiskWriteRequest { size } => {
                println!("{} [{}] received DiskWriteRequest from {}", ctx.time(), ctx.id, from);
                ctx.emit(DiskWriteCompleted { src_event_id: ctx.event_id }, from, self.calc_write_delay(*size));
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}