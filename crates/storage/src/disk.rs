use core::context::SimulationContext;

use crate::api::{DataReadCompleted, DataWriteCompleted};

pub struct Disk {
    capacity: u64,
    used: u64,
    read_bandwidth: u64,
    write_bandwidth: u64,
    ready_time: f64,
    next_request_id: u64,
    ctx: SimulationContext,
}

impl Disk {
    pub fn new(capacity: u64, read_bandwidth: u64, write_bandwidth: u64, ctx: SimulationContext) -> Self {
        Self {
            capacity,
            used: 0,
            read_bandwidth,
            write_bandwidth,
            ready_time: 0.,
            next_request_id: 0,
            ctx,
        }
    }

    pub fn read<S: Into<String>>(&mut self, size: u64, requester: S) -> u64 {
        let requester = requester.into();

        println!(
            "{} [{}] disk READ {} bytes request from {}",
            self.ctx.time(),
            self.ctx.id(),
            size,
            &requester,
        );

        let read_time = size as f64 / self.read_bandwidth as f64;
        self.ready_time = self.ready_time.max(self.ctx.time()) + read_time;

        let request_id = self.next_request_id;
        self.next_request_id += 1;

        self.ctx.emit(
            DataReadCompleted {
                request_id: request_id,
                size: size,
            },
            requester,
            self.ready_time - self.ctx.time(),
        );

        request_id
    }

    pub fn write<S: Into<String>>(&mut self, size: u64, requester: S) -> u64 {
        let requester = requester.into();

        println!(
            "{} [{}] disk WRITE {} bytes request from {}",
            self.ctx.time(),
            self.ctx.id(),
            size,
            requester,
        );

        let remaining = self.capacity - self.used;
        let size_to_write = if remaining < size { remaining } else { size };
        self.used += size_to_write;

        let write_time = size_to_write as f64 / self.write_bandwidth as f64;
        self.ready_time = self.ready_time.max(self.ctx.time()) + write_time;

        let request_id = self.next_request_id;
        self.next_request_id += 1;

        self.ctx.emit(
            DataWriteCompleted {
                request_id: request_id,
                size: size_to_write,
            },
            requester,
            self.ready_time - self.ctx.time(),
        );

        request_id
    }

    pub fn get_used_space(&self) -> u64 {
        self.used
    }
}
