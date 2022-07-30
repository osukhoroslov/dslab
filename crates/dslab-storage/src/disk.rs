//! Disk model.

use sugars::boxed;

use dslab_core::component::Id;
use dslab_core::{context::SimulationContext, log_debug, log_error};

use crate::bandwidth::{BWModel, ConstantBWModel};
use crate::events::{DataReadCompleted, DataReadFailed, DataWriteCompleted, DataWriteFailed};

/// Representation of disk.
pub struct Disk {
    capacity: u64,
    used: u64,
    read_bw_model: Box<dyn BWModel>,
    write_bw_model: Box<dyn BWModel>,
    ready_time: f64,
    next_request_id: u64,
    ctx: SimulationContext,
}

impl Disk {
    /// Creates new disk.
    pub fn new(
        capacity: u64,
        read_bw_model: Box<dyn BWModel>,
        write_bw_model: Box<dyn BWModel>,
        ctx: SimulationContext,
    ) -> Self {
        Self {
            capacity,
            used: 0,
            read_bw_model,
            write_bw_model,
            ready_time: 0.,
            next_request_id: 0,
            ctx,
        }
    }

    /// Creates new disk with constant bandwidth model.
    pub fn new_simple(capacity: u64, read_bandwidth: u64, write_bandwidth: u64, ctx: SimulationContext) -> Self {
        Self::new(
            capacity,
            boxed!(ConstantBWModel::new(read_bandwidth)),
            boxed!(ConstantBWModel::new(write_bandwidth)),
            ctx,
        )
    }

    fn get_unique_request_id(&mut self) -> u64 {
        let request_id = self.next_request_id;
        self.next_request_id += 1;
        request_id
    }

    /// Requests reading from disk of `size`. Emits response event to `requester`. Returns `request_id` which is unique to this disk.
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
            let bw = self.read_bw_model.get_bandwidth(size, &mut self.ctx);
            log_debug!(self.ctx, "Read bandwidth: {}", bw);
            let read_time = size as f64 / bw as f64;
            self.ready_time = self.ready_time.max(self.ctx.time()) + read_time;
            self.ctx.emit(
                DataReadCompleted { request_id, size },
                requester,
                self.ready_time - self.ctx.time(),
            );
        }
        request_id
    }

    /// Requests writing from disk of `size`. Emits response event to `requester`. Returns `request_id` which is unique to this disk.
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
            let bw = self.write_bw_model.get_bandwidth(size, &mut self.ctx);
            log_debug!(self.ctx, "Write bandwidth: {}", bw);
            let write_time = size as f64 / bw as f64;
            self.ready_time = self.ready_time.max(self.ctx.time()) + write_time;
            self.ctx.emit(
                DataWriteCompleted { request_id, size },
                requester,
                self.ready_time - self.ctx.time(),
            );
        }
        request_id
    }

    /// Marks `size` as free. Given `size` should not be more than used space.
    pub fn mark_free(&mut self, size: u64) -> Result<(), String> {
        if size <= self.used {
            self.used -= size;
            return Ok(());
        }
        Err(format!("invalid size: {}", size))
    }

    /// Returns amount of used space on disk.
    pub fn get_used_space(&self) -> u64 {
        self.used
    }

    /// Returns id of this disk.
    pub fn id(&self) -> Id {
        self.ctx.id()
    }
}
