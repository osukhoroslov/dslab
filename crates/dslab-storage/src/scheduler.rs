//! Disk I/O schedulers.

use std::collections::{HashMap, VecDeque};

use dslab_core::SimulationContext;
use dslab_models::throughput_sharing::{FairThroughputSharingModel, ThroughputSharingModel};

use crate::disk::{DiskOperation, DiskOperationCompleted, DiskOperationType};

/// A trait for disk I/O scheduler which manages the execution of disk operations.
///
/// It accepts operations from [`Disk`](crate::disk::Disk) and passes them to the underlying throughput models
/// via some logic. For example, scheduler can limit the number of concurrent operations.
///
/// It is assumed that scheduler does not receive operation completion events and should be notified
/// about them explicitly via [`complete`] method.
pub(crate) trait Scheduler {
    /// Adds new operation to the scheduler.
    fn submit(&mut self, operation: DiskOperation, ctx: &mut SimulationContext);

    /// A method for notifying the scheduler about the operation completion.
    ///
    /// Returns the corresponding completed operation.
    fn complete(&mut self, request_id: u64, ctx: &mut SimulationContext) -> DiskOperation;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A scheduler which dispatches operations in FIFO order.
///
/// Uses independent throughput models for read and write operations.
/// Supports limits on the number of concurrent operations (total and per operation type).
pub struct FifoScheduler {
    read_model: ThroughputModelWithOpsLimit,
    write_model: ThroughputModelWithOpsLimit,
    total_ops_limit: Option<u64>,
    total_ops_count: u64,
    pending_ops: VecDeque<DiskOperation>,
    operation_types: HashMap<u64, DiskOperationType>,
}

impl FifoScheduler {
    /// Creates FIFO scheduler with given throughput models and concurrent operations limits.
    pub fn new(
        read_throughput_model: FairThroughputSharingModel<DiskOperation>,
        write_throughput_model: FairThroughputSharingModel<DiskOperation>,
        total_ops_limit: Option<u64>,
        read_ops_limit: Option<u64>,
        write_ops_limit: Option<u64>,
    ) -> Self {
        assert!(
            read_ops_limit.is_none() || read_ops_limit.unwrap() > 0,
            "Zero concurrent read operations limit is useless"
        );
        assert!(
            write_ops_limit.is_none() || write_ops_limit.unwrap() > 0,
            "Zero concurrent write operations limit is useless"
        );
        assert!(
            total_ops_limit.is_none() || total_ops_limit.unwrap() > 0,
            "Zero concurrent operations limit is useless"
        );

        Self {
            read_model: ThroughputModelWithOpsLimit::new(read_throughput_model, read_ops_limit),
            write_model: ThroughputModelWithOpsLimit::new(write_throughput_model, write_ops_limit),
            total_ops_limit,
            total_ops_count: 0,
            pending_ops: VecDeque::new(),
            operation_types: HashMap::new(),
        }
    }

    fn try_schedule(&mut self, ctx: &mut SimulationContext) {
        if let Some(total_limit) = self.total_ops_limit {
            if self.total_ops_count >= total_limit {
                return;
            }
        }
        if let Some(operation) = self.pending_ops.pop_front() {
            let model = match operation.op_type {
                DiskOperationType::Read => &mut self.read_model,
                DiskOperationType::Write => &mut self.write_model,
            };
            model.submit(operation, ctx);
            self.total_ops_count += 1;
        }
    }
}

impl Scheduler for FifoScheduler {
    fn submit(&mut self, operation: DiskOperation, ctx: &mut SimulationContext) {
        self.operation_types
            .insert(operation.request_id, operation.op_type.clone());
        self.pending_ops.push_back(operation);
        self.try_schedule(ctx);
    }

    fn complete(&mut self, request_id: u64, ctx: &mut SimulationContext) -> DiskOperation {
        let model = match self.operation_types.remove(&request_id).unwrap() {
            DiskOperationType::Read => &mut self.read_model,
            DiskOperationType::Write => &mut self.write_model,
        };
        let (time, operation) = model.complete(ctx);
        debug_assert!(ctx.time() == time, "Unexpected operation completion time");
        self.total_ops_count -= 1;
        self.try_schedule(ctx);
        operation
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ThroughputModelWithOpsLimit {
    inner_throughput_model: FairThroughputSharingModel<DiskOperation>,
    concurrent_ops_limit: Option<u64>,
    concurrent_ops_count: u64,
    pending_ops: VecDeque<(DiskOperation, f64)>,
    next_event: Option<u64>,
}

impl ThroughputModelWithOpsLimit {
    fn new(throughput_model: FairThroughputSharingModel<DiskOperation>, concurrent_ops_limit: Option<u64>) -> Self {
        Self {
            inner_throughput_model: throughput_model,
            concurrent_ops_limit,
            concurrent_ops_count: 0,
            pending_ops: VecDeque::new(),
            next_event: None,
        }
    }

    fn submit(&mut self, operation: DiskOperation, ctx: &mut SimulationContext) {
        let volume = operation.size as f64;
        if let Some(limit) = self.concurrent_ops_limit {
            if self.concurrent_ops_count >= limit {
                self.pending_ops.push_back((operation, volume));
                return;
            }
        }
        self.submit_to_throughput_model(operation, volume, ctx);
        self.emit_next_event(ctx);
    }

    fn complete(&mut self, ctx: &mut SimulationContext) -> (f64, DiskOperation) {
        let result = self.inner_throughput_model.pop().unwrap();
        self.concurrent_ops_count -= 1;
        self.next_event = None;
        if let Some((operation, volume)) = self.pending_ops.pop_front() {
            self.submit_to_throughput_model(operation, volume, ctx);
        }
        self.emit_next_event(ctx);
        result
    }

    fn submit_to_throughput_model(&mut self, operation: DiskOperation, volume: f64, ctx: &mut SimulationContext) {
        self.inner_throughput_model.insert(operation, volume, ctx);
        self.concurrent_ops_count += 1;
    }

    fn emit_next_event(&mut self, ctx: &mut SimulationContext) {
        if let Some((time, operation)) = self.inner_throughput_model.peek() {
            if let Some(next_event) = self.next_event {
                ctx.cancel_event(next_event);
            }
            self.next_event = Some(ctx.emit_self(
                DiskOperationCompleted {
                    request_id: operation.request_id,
                },
                time - ctx.time(),
            ));
        }
    }
}
