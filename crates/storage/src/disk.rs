use core::context::SimulationContext;
use core::log_debug;

use crate::api::{DataReadCompleted, DataReadFailed, DataWriteCompleted, DataWriteFailed};

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

        let request_id = self.next_request_id;
        self.next_request_id += 1;

        log_debug!(self.ctx, "Requested read {} bytes request from {}", size, &requester);

        if size > self.capacity {
            log_debug!(
                self.ctx,
                "Size {} is more than capacity {}, failing",
                size,
                self.capacity,
            );
            self.ctx.emit_now(
                DataReadFailed {
                    request_id,
                    error: "requested size > capacity".to_string(),
                },
                requester,
            );
        } else {
            let read_time = size as f64 / self.read_bandwidth as f64;
            self.ready_time = self.ready_time.max(self.ctx.time()) + read_time;

            self.ctx.emit(
                DataReadCompleted { request_id, size },
                requester,
                self.ready_time - self.ctx.time(),
            );
        }

        request_id
    }

    pub fn write<S: Into<String>>(&mut self, size: u64, requester: S) -> u64 {
        let requester = requester.into();

        let request_id = self.next_request_id;
        self.next_request_id += 1;

        log_debug!(self.ctx, "Requested write {} bytes request from {}", size, requester);

        if self.capacity - self.used < size {
            log_debug!(
                self.ctx,
                "Not enough space to write {} bytes, only {} available, failing",
                size,
                self.capacity - self.used,
            );

            self.ctx.emit_now(
                DataWriteFailed {
                    request_id,
                    error: "requested size > free space".to_string(),
                },
                requester,
            );
        } else {
            self.used += size;
            let write_time = size as f64 / self.write_bandwidth as f64;
            self.ready_time = self.ready_time.max(self.ctx.time()) + write_time;

            self.ctx.emit(
                DataWriteCompleted {
                    request_id: request_id,
                    size: size,
                },
                requester,
                self.ready_time - self.ctx.time(),
            );
        }

        request_id
    }

    pub fn mark_free(&mut self, size: u64) -> bool {
        if size <= self.used {
            self.used -= size;
            return true;
        }
        false
    }

    pub fn get_used_space(&self) -> u64 {
        self.used
    }

    pub fn id(&self) -> &str {
        self.ctx.id()
    }
}
