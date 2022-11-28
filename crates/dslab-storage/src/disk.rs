//! A simple disk model, which processes read/write requests sequentially.
//!
//! It has two main methods - [`read`](Disk::read) and [`write`](Disk::write),
//! and some utility functions as [`mark_free`](Disk::mark_free) or [`get_used_space`](Disk::get_used_space).
//! It can be created by [`new_simple`](Disk::new_simple) function if bandwidths are fixed.
//! There is also support for [bandwidth models](crate::bandwidth) that dynamically compute per-request bandwidth based
//! on the request size, current simulation time, etc. Several implementations of these models are included in this
//! crate, and other user-defined models can also be used. This model of disk **does not** support bandwidth sharing, so
//! disk can process only one request at a time. The requests are processed sequentially in FIFO order.
//!
//! Usage example can be found in `/examples/storage-disk`.

use sugars::boxed;

use dslab_core::component::Id;
use dslab_core::{context::SimulationContext, log_debug, log_error};

use crate::bandwidth::{BWModel, ConstantBWModel};
use crate::events::{DataReadCompleted, DataReadFailed, DataWriteCompleted, DataWriteFailed};

/// Representation of disk.
///
/// Disk is characterized by its capacity and read/write bandwidths (represented by bandwidth models).
/// Disk state includes the amount of used disk space and the completion time of last pending activity (`ready_time`).
pub struct Disk {
    capacity: u64,
    used: u64,
    read_bw_model: Box<dyn BWModel>,
    write_bw_model: Box<dyn BWModel>,
    ready_time: f64,
    next_request_id: u64,
    ctx: SimulationContext,
}

/// Information about disk, including its capacity and current usage.
#[derive(Debug, PartialEq)]
pub struct DiskInfo {
    /// Disk capacity. Is equal to `used_space` + `free_space`.
    pub capacity: u64,
    /// Amount of used space. Cannot be greater than `capacity`.
    pub used_space: u64,
    /// Amount of free space. Cannot be greater than `capacity`.
    pub free_space: u64,
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

    fn make_unique_request_id(&mut self) -> u64 {
        let request_id = self.next_request_id;
        self.next_request_id += 1;
        request_id
    }

    /// Submits data read request and returns unique request id.
    ///
    /// The amount of data read from disk is specified in `size`.
    /// The component specified in `requester` will receive `DataReadCompleted` event upon the read completion. If the
    /// read size is larger than the disk capacity, `DataReadFailed` event will be immediately emitted instead. Note
    /// that the returned request id is unique only within the current disk.
    pub fn read(&mut self, size: u64, requester: Id) -> u64 {
        log_debug!(
            self.ctx,
            "Received read request, size: {}, requester: {}",
            size,
            requester
        );
        let request_id = self.make_unique_request_id();
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

    /// Submits data write request and returns unique request id.
    ///
    /// The amount of data written to disk is specified in `size`.
    /// The component specified in `requester` will receive `DataWriteCompleted` event upon the write completion. If
    /// there is not enough available disk space, `DataWriteFailed` event will be immediately emitted instead.
    /// Note that the returned request id is unique only within the current disk.
    pub fn write(&mut self, size: u64, requester: Id) -> u64 {
        let request_id = self.make_unique_request_id();
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

    /// Marks previously used disk space of given `size` as free.
    ///
    /// The `size` should not exceed the currently used disk space.
    pub fn mark_free(&mut self, size: u64) -> Result<(), String> {
        if size <= self.used {
            self.used -= size;
            return Ok(());
        }
        Err(format!("invalid size: {}", size))
    }

    /// Returns the amount of used disk space.
    pub fn used_space(&self) -> u64 {
        self.used
    }

    /// Returns the amount of free disk space.
    pub fn free_space(&self) -> u64 {
        self.capacity - self.used
    }

    /// Returns the capacity of disk.
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Returns id of this disk.
    pub fn id(&self) -> Id {
        self.ctx.id()
    }

    /// Returns struct with information about the disk.
    pub fn info(&self) -> DiskInfo {
        DiskInfo {
            capacity: self.capacity(),
            used_space: self.used_space(),
            free_space: self.free_space(),
        }
    }
}
