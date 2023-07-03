//! Disk schedulers definition.

use std::collections::VecDeque;

use dslab_core::{cast, event::Event, SimulationContext};
use dslab_models::throughput_sharing::{FairThroughputSharingModel, ThroughputSharingModel};

use crate::disk::{DiskActivity, DiskActivityCompleted, DiskActivityKind};
use crate::events::{DataReadCompleted, DataWriteCompleted};

/// An entity which accepts operations from the disk and passes them to throughput models via some logic.
/// For example, scheduler can limit concurrent operations count.
pub(crate) trait Scheduler {
    /// Adds new operation to scheduler.
    fn submit(&mut self, kind: DiskActivityKind, item: DiskActivity, volume: f64, ctx: &mut SimulationContext);
    /// A method for passing events which [`crate::disk::Disk`] received from the simulation.
    fn notify_on_event(&mut self, event: Event, ctx: &mut SimulationContext);
}

///////////////////////////////////////////////////////////////////////////////

/// Implementation of a simple FIFO [`Scheduler`] with independent reads and writes.
/// User can provide a limit to concurrent operations count.
pub struct FifoScheduler {
    read_model: ThroughputModelWithOpsLimit,
    write_model: ThroughputModelWithOpsLimit,
}

impl FifoScheduler {
    /// Creates fair scheduler with given throughput models and limits on concurrent operations count.
    pub fn new(
        read_throughput_model: FairThroughputSharingModel<DiskActivity>,
        concurrent_read_ops_limit: Option<u64>,
        write_throughput_model: FairThroughputSharingModel<DiskActivity>,
        concurrent_write_ops_limit: Option<u64>,
    ) -> Self {
        debug_assert!(
            concurrent_read_ops_limit.is_none() || concurrent_read_ops_limit.unwrap() > 0,
            "Zero operations limit is useless"
        );
        debug_assert!(
            concurrent_write_ops_limit.is_none() || concurrent_write_ops_limit.unwrap() > 0,
            "Zero operations limit is useless"
        );

        Self {
            read_model: ThroughputModelWithOpsLimit::new(
                DiskActivityKind::Read,
                read_throughput_model,
                concurrent_read_ops_limit,
            ),
            write_model: ThroughputModelWithOpsLimit::new(
                DiskActivityKind::Write,
                write_throughput_model,
                concurrent_write_ops_limit,
            ),
        }
    }

    fn on_read_completed(&mut self, ctx: &mut SimulationContext) {
        let (_, activity) = self.read_model.pop(ctx).unwrap();
        ctx.emit_now(
            DataReadCompleted {
                request_id: activity.request_id,
                size: activity.size,
            },
            activity.requester,
        );
        self.read_model.emit_next_event(ctx);
    }

    fn on_write_completed(&mut self, ctx: &mut SimulationContext) {
        let (_, activity) = self.write_model.pop(ctx).unwrap();
        ctx.emit_now(
            DataWriteCompleted {
                request_id: activity.request_id,
                size: activity.size,
            },
            activity.requester,
        );
        self.write_model.emit_next_event(ctx);
    }
}

impl Scheduler for FifoScheduler {
    fn submit(&mut self, kind: DiskActivityKind, item: DiskActivity, volume: f64, ctx: &mut SimulationContext) {
        match kind {
            DiskActivityKind::Read => self.read_model.insert(item, volume, ctx),
            DiskActivityKind::Write => self.write_model.insert(item, volume, ctx),
        }
    }

    fn notify_on_event(&mut self, event: Event, ctx: &mut SimulationContext) {
        cast!(match event.data {
            DiskActivityCompleted { kind } => {
                match kind {
                    DiskActivityKind::Read => self.on_read_completed(ctx),
                    DiskActivityKind::Write => self.on_write_completed(ctx),
                }
            }
        })
    }
}

///////////////////////////////////////////////////////////////////////////////

struct ThroughputModelWithOpsLimit {
    kind: DiskActivityKind,
    inner_throughput_model: FairThroughputSharingModel<DiskActivity>,
    concurrent_ops_limit: Option<u64>,
    scheduled_ops_count: u64,
    pending_ops: VecDeque<(DiskActivity, f64)>,
    next_event: u64,
}

impl ThroughputModelWithOpsLimit {
    fn new(
        kind: DiskActivityKind,
        throughput_model: FairThroughputSharingModel<DiskActivity>,
        concurrent_ops_limit: Option<u64>,
    ) -> Self {
        Self {
            kind,
            inner_throughput_model: throughput_model,
            concurrent_ops_limit,
            scheduled_ops_count: 0,
            pending_ops: VecDeque::new(),
            next_event: u64::MAX,
        }
    }

    fn submit_to_throughput_model(&mut self, item: DiskActivity, volume: f64, ctx: &mut SimulationContext) {
        self.inner_throughput_model.insert(item, volume, ctx);
        self.scheduled_ops_count += 1;
    }

    fn emit_next_event(&mut self, ctx: &mut SimulationContext) {
        if let Some((time, _)) = self.inner_throughput_model.peek() {
            self.next_event = ctx.emit_self(
                DiskActivityCompleted {
                    kind: self.kind.clone(),
                },
                time - ctx.time(),
            );
        }
    }

    fn insert(&mut self, item: DiskActivity, volume: f64, ctx: &mut SimulationContext) {
        if let Some(limit) = self.concurrent_ops_limit {
            if self.scheduled_ops_count >= limit {
                self.pending_ops.push_back((item, volume));
                return;
            }
        }
        self.submit_to_throughput_model(item, volume, ctx);
        ctx.cancel_event(self.next_event);
        self.emit_next_event(ctx);
    }

    fn pop(&mut self, ctx: &mut SimulationContext) -> Option<(f64, DiskActivity)> {
        let result = self.inner_throughput_model.pop();
        if result.is_some() {
            self.scheduled_ops_count -= 1;
            if let Some((item, volume)) = self.pending_ops.pop_front() {
                self.submit_to_throughput_model(item, volume, ctx);
            }
        }
        result
    }
}
