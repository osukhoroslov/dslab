use core::cast;
use core::event::Event;
use core::{context::SimulationContext, handler::EventHandler};

use crate::api::{DataReadCompleted, DataReadRequest, DataWriteCompleted, DataWriteRequest};

pub struct Disk {
    ctx: SimulationContext,
    capacity: u64,
    used: u64,
    read_bandwidth: u64,
    write_bandwidth: u64,
    ready_time: f64,
}

impl Disk {
    pub fn new(ctx: SimulationContext, capacity: u64, read_bandwidth: u64, write_bandwidth: u64) -> Self {
        Self {
            ctx,
            capacity,
            used: 0,
            read_bandwidth,
            write_bandwidth,
            ready_time: 0.,
        }
    }

    pub fn get_used_space(&self) -> u64 {
        self.used
    }

    pub fn id(&self) -> &str {
        self.ctx.id()
    }
}

impl EventHandler for Disk {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            DataReadRequest { size } => {
                println!(
                    "{} [{}] disk READ {} bytes request from {}",
                    self.ctx.time(),
                    self.ctx.id(),
                    size,
                    event.src,
                );

                let read_time = size as f64 / self.read_bandwidth as f64;
                self.ready_time = self.ready_time.max(self.ctx.time()) + read_time;

                self.ctx.emit(
                    DataReadCompleted {
                        src_event_id: event.id,
                        size: size,
                    },
                    event.src,
                    self.ready_time - self.ctx.time(),
                );
            }
            DataWriteRequest { size } => {
                println!(
                    "{} [{}] disk WRITE {} bytes request from {}",
                    self.ctx.time(),
                    self.ctx.id(),
                    size,
                    event.src,
                );

                let remaining = self.capacity - self.used;
                let size_to_write = if remaining < size { remaining } else { size };
                self.used += size_to_write;

                let write_time = size_to_write as f64 / self.write_bandwidth as f64;
                self.ready_time = self.ready_time.max(self.ctx.time()) + write_time;

                self.ctx.emit(
                    DataWriteCompleted {
                        src_event_id: event.id,
                        size: size_to_write,
                    },
                    event.src,
                    self.ready_time - self.ctx.time(),
                );
            }
        })
    }
}
