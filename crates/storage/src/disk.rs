use core::component::Id;
use core::{context::SimulationContext, log_debug, log_error};

use crate::events::{DataReadCompleted, DataReadFailed, DataWriteCompleted, DataWriteFailed};

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

    fn get_unique_request_id(&mut self) -> u64 {
        let request_id = self.next_request_id;
        self.next_request_id += 1;
        request_id
    }

    pub fn read(&mut self, size: u64, requester: Id) -> u64 {
        log_debug!(
            self.ctx,
            "Received read request, size: {}, requester: {}",
            size,
            requester
        );
        let request_id = self.get_unique_request_id();
        if size > self.capacity {
            let error = format!(
                "requested read size is {} but only {} is available",
                size, self.capacity
            );
            log_error!(self.ctx, "Failed reading: {}", error,);
            self.ctx.emit_now(DataReadFailed { request_id, error }, requester);
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

    pub fn write(&mut self, size: u64, requester: Id) -> u64 {
        let request_id = self.get_unique_request_id();
        log_debug!(
            self.ctx,
            "Received write request, size: {}, requester: {}",
            size,
            requester
        );
        let available = self.capacity - self.used;
        if available < size {
            let error = format!("requested write size is {} but only {} is available", size, available);
            log_error!(self.ctx, "Failed writing: {}", error,);
            self.ctx.emit_now(DataWriteFailed { request_id, error }, requester);
        } else {
            self.used += size;
            let write_time = size as f64 / self.write_bandwidth as f64;
            self.ready_time = self.ready_time.max(self.ctx.time()) + write_time;
            self.ctx.emit(
                DataWriteCompleted { request_id, size },
                requester,
                self.ready_time - self.ctx.time(),
            );
        }
        request_id
    }

    pub fn mark_free(&mut self, size: u64) -> Result<(), String> {
        if size <= self.used {
            self.used -= size;
            return Ok(());
        }
        Err(format!("invalid size: {}", size))
    }

    pub fn get_used_space(&self) -> u64 {
        self.used
    }

    pub fn id(&self) -> Id {
        self.ctx.id()
    }
}
