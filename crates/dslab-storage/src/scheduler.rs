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
    fn submit(&mut self, operation: DiskOperation, volume: f64, ctx: &mut SimulationContext);

    /// A method for notifying the scheduler about the operation completion.
    ///
    /// Returns the corresponding completed operation.
    fn complete(&mut self, request_id: u64, ctx: &mut SimulationContext) -> DiskOperation;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A simple FIFO [`Scheduler`] with independent throughput models for reads and writes.
/// Supports limiting the number of concurrent operations of each type.
pub struct FifoScheduler {
    read_model: ThroughputModelWithOpsLimit,
    write_model: ThroughputModelWithOpsLimit,
    operation_types: HashMap<u64, DiskOperationType>,
}

impl FifoScheduler {
    /// Creates FIFO scheduler with given throughput models and concurrent operations limits.
    pub fn new(
        read_throughput_model: FairThroughputSharingModel<DiskOperation>,
        concurrent_read_ops_limit: Option<u64>,
        write_throughput_model: FairThroughputSharingModel<DiskOperation>,
        concurrent_write_ops_limit: Option<u64>,
    ) -> Self {
        assert!(
            concurrent_read_ops_limit.is_none() || concurrent_read_ops_limit.unwrap() > 0,
            "Zero concurrent read operations limit is useless"
        );
        assert!(
            concurrent_write_ops_limit.is_none() || concurrent_write_ops_limit.unwrap() > 0,
            "Zero concurrent write operations limit is useless"
        );

        Self {
            read_model: ThroughputModelWithOpsLimit::new(read_throughput_model, concurrent_read_ops_limit),
            write_model: ThroughputModelWithOpsLimit::new(write_throughput_model, concurrent_write_ops_limit),
            operation_types: HashMap::new(),
        }
    }
}

impl Scheduler for FifoScheduler {
    fn submit(&mut self, operation: DiskOperation, volume: f64, ctx: &mut SimulationContext) {
        self.operation_types
            .insert(operation.request_id, operation.op_type.clone());
        match operation.op_type {
            DiskOperationType::Read => self.read_model.submit(operation, volume, ctx),
            DiskOperationType::Write => self.write_model.submit(operation, volume, ctx),
        }
    }

    fn complete(&mut self, request_id: u64, ctx: &mut SimulationContext) -> DiskOperation {
        let model = match self.operation_types.remove(&request_id).unwrap() {
            DiskOperationType::Read => &mut self.read_model,
            DiskOperationType::Write => &mut self.write_model,
        };
        let (time, operation) = model.complete(ctx).unwrap();
        debug_assert!(ctx.time() == time, "Unexpected operation completion time");
        operation
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ThroughputModelWithOpsLimit {
    inner_throughput_model: FairThroughputSharingModel<DiskOperation>,
    concurrent_ops_limit: Option<u64>,
    concurrent_ops_count: u64,
    pending_ops: VecDeque<(DiskOperation, f64)>,
    next_event: u64,
}

impl ThroughputModelWithOpsLimit {
    fn new(throughput_model: FairThroughputSharingModel<DiskOperation>, concurrent_ops_limit: Option<u64>) -> Self {
        Self {
            inner_throughput_model: throughput_model,
            concurrent_ops_limit,
            concurrent_ops_count: 0,
            pending_ops: VecDeque::new(),
            next_event: u64::MAX,
        }
    }

    fn submit(&mut self, operation: DiskOperation, volume: f64, ctx: &mut SimulationContext) {
        if let Some(limit) = self.concurrent_ops_limit {
            if self.concurrent_ops_count >= limit {
                self.pending_ops.push_back((operation, volume));
                return;
            }
        }
        self.submit_to_throughput_model(operation, volume, ctx);
        ctx.cancel_event(self.next_event);
        self.emit_next_event(ctx);
    }

    fn complete(&mut self, ctx: &mut SimulationContext) -> Option<(f64, DiskOperation)> {
        let result = self.inner_throughput_model.pop();
        if result.is_some() {
            self.concurrent_ops_count -= 1;
            if let Some((operation, volume)) = self.pending_ops.pop_front() {
                self.submit_to_throughput_model(operation, volume, ctx);
            }
            self.emit_next_event(ctx);
        }
        result
    }

    fn submit_to_throughput_model(&mut self, operation: DiskOperation, volume: f64, ctx: &mut SimulationContext) {
        self.inner_throughput_model.insert(operation, volume, ctx);
        self.concurrent_ops_count += 1;
    }

    fn emit_next_event(&mut self, ctx: &mut SimulationContext) {
        if let Some((time, operation)) = self.inner_throughput_model.peek() {
            self.next_event = ctx.emit_self(
                DiskOperationCompleted {
                    request_id: operation.request_id,
                },
                time - ctx.time(),
            );
        }
    }
}
