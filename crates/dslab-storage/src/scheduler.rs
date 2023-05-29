//! Disk schedulers definition.

use std::collections::VecDeque;

use dslab_core::SimulationContext;
use dslab_models::throughput_sharing::{FairThroughputSharingModel, ThroughputSharingModel};

/// An entity which accepts operations from the disk and passes them to throughput model via some logic.
/// For example, scheduler can limit concurrent operations count.
pub trait Scheduler<T> {
    /// Adds new operation to scheduler.
    fn insert(&mut self, item: T, volume: f64, ctx: &mut SimulationContext);

    /// Returns the next operation completion time (if any) along with corresponding item.
    ///
    /// The returned operation is removed from the inner throughput model.
    ///
    /// If this operation exists and there are operations awaiting scheduling in the scheduler queue, then next operation is scheduled to the model.
    fn pop(&mut self, ctx: &mut SimulationContext) -> Option<(f64, T)>;

    /// Returns the next operation completion time (if any) along with corresponding item.
    ///
    /// In contrast to pop, the returned activity is not removed from the model.
    fn peek(&self) -> Option<(f64, &T)>;
}

/// Implementation of a fair [`Scheduler`].
/// Schedules operations to the throughput model in FIFO order.
/// User can provide a limit to concurrent operations count.
pub struct FairScheduler<T> {
    throughput_model: FairThroughputSharingModel<T>,
    concurrent_ops_limit: Option<u64>,

    scheduled_ops_count: u64,
    pending_ops: VecDeque<(T, f64)>,
}

impl<T> FairScheduler<T> {
    /// Creates fair scheduler with given throughput model and no limit on concurrent operations count.
    pub fn new(throughput_model: FairThroughputSharingModel<T>) -> Self {
        Self {
            throughput_model,
            concurrent_ops_limit: None,
            scheduled_ops_count: 0,
            pending_ops: VecDeque::new(),
        }
    }

    /// Creates fair scheduler with given throughput model and given limit on concurrent operations count.
    pub fn new_with_concurrent_ops_limit(throughput_model: FairThroughputSharingModel<T>, limit: u64) -> Self {
        debug_assert!(limit > 0, "Zero operations limit is useless");
        Self {
            throughput_model,
            concurrent_ops_limit: Some(limit),
            scheduled_ops_count: 0,
            pending_ops: VecDeque::new(),
        }
    }

    fn schedule(&mut self, item: T, volume: f64, ctx: &mut SimulationContext) {
        self.throughput_model.insert(item, volume, ctx);
        self.scheduled_ops_count += 1;
    }
}

impl<T> Scheduler<T> for FairScheduler<T> {
    fn insert(&mut self, item: T, volume: f64, ctx: &mut SimulationContext) {
        if let Some(limit) = self.concurrent_ops_limit {
            if self.scheduled_ops_count >= limit {
                self.pending_ops.push_back((item, volume));
                return;
            }
        }
        self.schedule(item, volume, ctx);
    }

    fn pop(&mut self, ctx: &mut SimulationContext) -> Option<(f64, T)> {
        let result = self.throughput_model.pop();
        if result.is_some() {
            self.scheduled_ops_count -= 1;
            if let Some((item, volume)) = self.pending_ops.pop_front() {
                self.schedule(item, volume, ctx);
            }
        }
        result
    }

    fn peek(&self) -> Option<(f64, &T)> {
        self.throughput_model.peek()
    }
}
