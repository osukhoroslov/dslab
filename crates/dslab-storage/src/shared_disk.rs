//! Shared disk model.
//!
//! This is an alternative disk model that supports concurrent execution of requests with bandwidth sharing.
//! It uses the generic fair throughput sharing model from `dslab-models` to compute the request completion times.
//! Methods set is the same as for simple disk model.
//!
//! Usage example can be found in `/examples/storage-shared-disk`
//! Benchmark can be found in `/examples/storage-shared-disk-benchmark` and `/examples-other/simgrid/storage`

use serde::Serialize;

use dslab_core::cast;
use dslab_core::component::Id;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;
use dslab_core::{context::SimulationContext, log_debug, log_error};

use dslab_models::throughput_sharing::{FairThroughputSharingModel, ThroughputFunction, ThroughputSharingModel};

use crate::events::{DataReadCompleted, DataReadFailed, DataWriteCompleted, DataWriteFailed};

#[derive(Clone)]
struct DiskActivity {
    request_id: u64,
    requester: Id,
    size: u64,
}

#[derive(Serialize)]
struct DiskReadActivityCompleted {}

#[derive(Serialize)]
struct DiskWriteActivityCompleted {}

/// Representation of shared disk.
///
/// Shared disk is characterized by its capacity and read/write throughput models.
/// Shared disk state includes the amount of used disk space and state of throughput models.
pub struct SharedDisk {
    capacity: u64,
    used: u64,
    read_throughput_model: FairThroughputSharingModel<DiskActivity>,
    write_throughput_model: FairThroughputSharingModel<DiskActivity>,
    next_request_id: u64,
    next_read_event: u64,
    next_write_event: u64,
    ctx: SimulationContext,
}

impl SharedDisk {
    /// Creates new shared disk with fixed read and write throughput.
    pub fn new_simple(capacity: u64, read_bandwidth: f64, write_bandwidth: f64, ctx: SimulationContext) -> Self {
        Self {
            capacity,
            used: 0,
            read_throughput_model: FairThroughputSharingModel::with_fixed_throughput(read_bandwidth as f64),
            write_throughput_model: FairThroughputSharingModel::with_fixed_throughput(write_bandwidth as f64),
            next_request_id: 0,
            next_read_event: 0,
            next_write_event: 0,
            ctx,
        }
    }

    /// Creates new shared disk with given read and write throughput functions.
    pub fn new(
        capacity: u64,
        read_tf: ThroughputFunction,
        write_tf: ThroughputFunction,
        ctx: SimulationContext,
    ) -> Self {
        Self {
            capacity,
            used: 0,
            read_throughput_model: FairThroughputSharingModel::with_dynamic_throughput(read_tf),
            write_throughput_model: FairThroughputSharingModel::with_dynamic_throughput(write_tf),
            next_request_id: 0,
            next_read_event: 0,
            next_write_event: 0,
            ctx,
        }
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
        let request_id = self.get_unique_request_id();
        if size > self.capacity {
            let error = format!(
                "requested read size is {} but only {} is available",
                size, self.capacity
            );
            log_error!(self.ctx, "Failed reading: {}", error,);
            self.ctx.emit_now(DataReadFailed { request_id, error }, requester);
        } else {
            self.read_throughput_model.insert(
                self.ctx.time(),
                size as f64,
                DiskActivity {
                    request_id,
                    requester,
                    size,
                },
            );
            self.ctx.cancel_event(self.next_read_event);
            self.schedule_next_read_event();
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
            self.write_throughput_model.insert(
                self.ctx.time(),
                size as f64,
                DiskActivity {
                    request_id,
                    requester,
                    size,
                },
            );
            self.ctx.cancel_event(self.next_write_event);
            self.schedule_next_write_event();
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
    pub fn get_used_space(&self) -> u64 {
        self.used
    }

    /// Returns the amount of free disk space.
    pub fn get_free_space(&self) -> u64 {
        self.capacity - self.used
    }

    /// Returns the capacity of disk.
    pub fn get_capacity(&self) -> u64 {
        self.capacity
    }

    /// Returns id of this disk.
    pub fn id(&self) -> Id {
        self.ctx.id()
    }

    fn get_unique_request_id(&mut self) -> u64 {
        let request_id = self.next_request_id;
        self.next_request_id += 1;
        request_id
    }

    fn schedule_next_read_event(&mut self) {
        if let Some((time, _)) = self.read_throughput_model.peek() {
            self.next_read_event = self.ctx.emit_self(DiskReadActivityCompleted {}, time - self.ctx.time());
        }
    }

    fn schedule_next_write_event(&mut self) {
        if let Some((time, _)) = self.read_throughput_model.peek() {
            self.next_write_event = self
                .ctx
                .emit_self(DiskWriteActivityCompleted {}, time - self.ctx.time());
        }
    }

    fn on_read_completed(&mut self) {
        let (_, activity) = self.read_throughput_model.pop().unwrap();
        self.ctx.emit_now(
            DataReadCompleted {
                request_id: activity.request_id,
                size: activity.size,
            },
            activity.requester,
        );
        self.schedule_next_read_event();
    }

    fn on_write_completed(&mut self) {
        let (_, activity) = self.write_throughput_model.pop().unwrap();
        self.ctx.emit_now(
            DataWriteCompleted {
                request_id: activity.request_id,
                size: activity.size,
            },
            activity.requester,
        );
        self.schedule_next_write_event();
    }
}

impl EventHandler for SharedDisk {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            DiskReadActivityCompleted {} => {
                self.on_read_completed();
            }
            DiskWriteActivityCompleted {} => {
                self.on_write_completed();
            }
        })
    }
}
