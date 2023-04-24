//! Basic implementation of storage model for disk.
//!
//! It supports modeling of concurrent execution of disk operations by means of generic fair throughput sharing model
//! from the `dslab-models` crate. The underlying model also supports modeling of throughput degradation, variability
//! and dependence on operation properties by means of user-defined throughput and factor functions. For detailed
//! information about these functions, please refer to documentation in `dslab-models` crate.
//!
//! Note that this model is quite generic and can be used to model other types of storage as well.

use serde::Serialize;
use sugars::boxed;

use dslab_core::cast;
use dslab_core::component::Id;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;
use dslab_core::{context::SimulationContext, log_debug, log_error};
use dslab_models::throughput_sharing::{
    make_constant_throughput_fn, ActivityFactorFn, ConstantFactorFn, FairThroughputSharingModel, ResourceThroughputFn,
    ThroughputSharingModel,
};

use crate::events::{DataReadCompleted, DataReadFailed, DataWriteCompleted, DataWriteFailed};
use crate::storage::{Storage, StorageInfo};

/// Describes a disk operation.
#[derive(Clone)]
pub struct DiskActivity {
    request_id: u64,
    requester: Id,
    size: u64,
}

#[derive(Clone, Serialize)]
struct DiskReadActivityCompleted {}

#[derive(Clone, Serialize)]
struct DiskWriteActivityCompleted {}

type DiskThroughputModel = FairThroughputSharingModel<DiskActivity>;

///////////////////////////////////////////////////////////////////////////////

/// Represents disk specification.
///
/// Is filled by user and then passed to [`Disk`] when it is created.
pub struct DiskSpec {
    pub(in crate::disk) capacity: u64,
    pub(in crate::disk) read_throughput_fn: ResourceThroughputFn,
    pub(in crate::disk) write_throughput_fn: ResourceThroughputFn,
    pub(in crate::disk) read_factor_fn: Box<dyn ActivityFactorFn<DiskActivity>>,
    pub(in crate::disk) write_factor_fn: Box<dyn ActivityFactorFn<DiskActivity>>,
}

/// An error type to be returned on invalid [`DiskSpec`].
#[derive(Debug, Clone)]
pub struct InvalidDiskSpecError {}

const DEFAULT_DISK_CAPACITY: u64 = 1;
const DEFAULT_DISK_READ_BW: f64 = 1.;
const DEFAULT_DISK_WRITE_BW: f64 = 1.;

impl Default for DiskSpec {
    /// Creates default disk specification.
    ///
    /// Capacity of the disk, its read and write bandwidths are set to 1 and to be constant.
    /// Read and write throughput functions are set to constant with multiplier = 1.
    ///
    /// After editing spec is passed to [`Disk`].
    fn default() -> Self {
        Self {
            capacity: DEFAULT_DISK_CAPACITY,
            read_throughput_fn: make_constant_throughput_fn(DEFAULT_DISK_READ_BW),
            write_throughput_fn: make_constant_throughput_fn(DEFAULT_DISK_WRITE_BW),
            read_throughput_factor_fn: boxed!(ConstantFactorFn::new(1.)),
            write_throughput_factor_fn: boxed!(ConstantFactorFn::new(1.)),
        }
    }
}

impl DiskSpec {
    /// Sets capaticy of the disk.
    pub fn set_capacity(&mut self, capacity: u64) -> &mut Self {
        self.capacity = capacity;
        self
    }

    /// Sets read bandwidth to be constant with given value.
    pub fn set_constant_read_bw(&mut self, read_bw: f64) -> &mut Self {
        self.read_throughput_fn = make_constant_throughput_fn(read_bw);
        self
    }

    /// Sets write bandwidth to be constant with given value.
    pub fn set_constant_write_bw(&mut self, write_bw: f64) -> &mut Self {
        self.write_throughput_fn = make_constant_throughput_fn(write_bw);
        self
    }

    /// Sets read throughput function to be constant with given value.
    pub fn set_read_throughput_fn(&mut self, read_throughput_fn: ResourceThroughputFn) -> &mut Self {
        self.read_throughput_fn = read_throughput_fn;
        self
    }

    /// Sets write throughput function to be constant with given value.
    pub fn set_write_throughput_fn(&mut self, write_throughput_fn: ResourceThroughputFn) -> &mut Self {
        self.write_throughput_fn = write_throughput_fn;
        self
    }

    /// Sets read throughput factor function to given functor.
    pub fn set_read_throughput_factor_fn(
        &mut self,
        read_throughput_factor_fn: Box<dyn ActivityFactorFn<DiskActivity>>,
    ) -> &mut Self {
        self.read_throughput_factor_fn = read_throughput_factor_fn;
        self
    }

    /// Sets write throughput factor function to given functor.
    pub fn set_write_throughput_factor_fn(
        &mut self,
        write_throughput_factor_fn: Box<dyn ActivityFactorFn<DiskActivity>>,
    ) -> &mut Self {
        self.write_throughput_factor_fn = write_throughput_factor_fn;
        self
    }
}

///////////////////////////////////////////////////////////////////////////////

/// Represents a disk.
///
/// Disk is characterized by its capacity and read/write throughput models.
///
/// Disk state includes the amount of used disk space and state of throughput models.
pub struct Disk {
    capacity: u64,
    used: u64,
    read_throughput_model: DiskThroughputModel,
    write_throughput_model: DiskThroughputModel,
    next_request_id: u64,
    next_read_event: u64,
    next_write_event: u64,
    ctx: SimulationContext,
}

impl Disk {
    /// Creates new disk from given spec.
    ///
    /// Returns [`InvalidDiskSpecError`] on invalid spec.
    pub fn new(spec: DiskSpec, ctx: SimulationContext) -> Result<Self, InvalidDiskSpecError> {
        Ok(Self {
            capacity: spec.capacity,
            used: 0,
            read_throughput_model: FairThroughputSharingModel::new(
                spec.read_throughput_fn,
                spec.read_throughput_factor_fn,
            ),
            write_throughput_model: FairThroughputSharingModel::new(
                spec.write_throughput_fn,
                spec.write_throughput_factor_fn,
            ),
            next_request_id: 0,
            next_read_event: u64::MAX,
            next_write_event: u64::MAX,
            ctx,
        })
    }

    fn make_unique_request_id(&mut self) -> u64 {
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
        if let Some((time, _)) = self.write_throughput_model.peek() {
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

/// Storage model implementation for disk.
impl Storage for Disk {
    fn read(&mut self, size: u64, requester: Id) -> u64 {
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
            self.read_throughput_model.insert(
                DiskActivity {
                    request_id,
                    requester,
                    size,
                },
                size as f64,
                &mut self.ctx,
            );
            self.ctx.cancel_event(self.next_read_event);
            self.schedule_next_read_event();
        }
        request_id
    }

    fn write(&mut self, size: u64, requester: Id) -> u64 {
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
            self.write_throughput_model.insert(
                DiskActivity {
                    request_id,
                    requester,
                    size,
                },
                size as f64,
                &mut self.ctx,
            );
            self.ctx.cancel_event(self.next_write_event);
            self.schedule_next_write_event();
        }
        request_id
    }

    fn mark_free(&mut self, size: u64) -> Result<(), String> {
        if size <= self.used {
            self.used -= size;
            return Ok(());
        }
        Err(format!("invalid size: {}", size))
    }

    fn used_space(&self) -> u64 {
        self.used
    }

    fn free_space(&self) -> u64 {
        self.capacity - self.used
    }

    fn capacity(&self) -> u64 {
        self.capacity
    }

    fn id(&self) -> Id {
        self.ctx.id()
    }

    fn info(&self) -> StorageInfo {
        StorageInfo {
            capacity: self.capacity(),
            used_space: self.used_space(),
            free_space: self.free_space(),
        }
    }
}

impl EventHandler for Disk {
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
