//! Shared disk model.

use serde::Serialize;

use dslab_core::cast;
use dslab_core::component::Id;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;
use dslab_core::{context::SimulationContext, log_debug, log_error};

use dslab_models::fair_sharing::FairThroughputSharingModel as ThroughputSharingModelImpl;
use dslab_models::model::{ThroughputFunction, ThroughputSharingModel};

use crate::events::{DataReadCompleted, DataReadFailed, DataWriteCompleted, DataWriteFailed};

#[derive(Clone)]
struct DiskActivity {
    request_id: u64,
    requester: Id,
    size: u64,
}

#[derive(Serialize)]
/// Event type for shared disk read request completed.
pub struct DiskReadActivityCompleted {}

#[derive(Serialize)]
/// Event type for shared disk write request completed.
pub struct DiskWriteActivityCompleted {}

/// Representation of shared disk.
pub struct SharedDisk {
    capacity: u64,
    used: u64,
    read_throughput_model: ThroughputSharingModelImpl<DiskActivity>,
    write_throughput_model: ThroughputSharingModelImpl<DiskActivity>,
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
            read_throughput_model: ThroughputSharingModelImpl::with_fixed_throughput(read_bandwidth as f64),
            write_throughput_model: ThroughputSharingModelImpl::with_fixed_throughput(write_bandwidth as f64),
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
            read_throughput_model: ThroughputSharingModelImpl::with_dynamic_throughput(read_tf),
            write_throughput_model: ThroughputSharingModelImpl::with_dynamic_throughput(write_tf),
            next_request_id: 0,
            next_read_event: 0,
            next_write_event: 0,
            ctx,
        }
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
