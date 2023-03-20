use std::collections::VecDeque;

use dslab_core::SimulationContext;
use dslab_models::throughput_sharing::{FairThroughputSharingModel, ThroughputSharingModel};

pub trait Scheduler<T> {
    fn insert(&mut self, item: T, volume: f64, ctx: &mut SimulationContext);
    fn pop(&mut self, ctx: &mut SimulationContext) -> Option<(f64, T)>;
    fn peek(&self) -> Option<(f64, &T)>;
}

pub struct FairScheduler<T> {
    throughput_model: FairThroughputSharingModel<T>,
    concurrent_operations_limit: Option<u64>,

    scheduled_operations_count: u64,
    pending_operations: VecDeque<(T, f64)>,
}

impl<T> FairScheduler<T> {
    pub fn new(throughput_model: FairThroughputSharingModel<T>) -> Self {
        Self {
            throughput_model,
            concurrent_operations_limit: None,
            scheduled_operations_count: 0,
            pending_operations: VecDeque::new(),
        }
    }

    pub fn new_with_concurrent_operations_limit(throughput_model: FairThroughputSharingModel<T>, limit: u64) -> Self {
        debug_assert!(limit > 0, "Zero operations limit is useless");
        Self {
            throughput_model,
            concurrent_operations_limit: Some(limit),
            scheduled_operations_count: 0,
            pending_operations: VecDeque::new(),
        }
    }

    fn schedule(&mut self, item: T, volume: f64, ctx: &mut SimulationContext) {
        self.throughput_model.insert(item, volume, ctx);
        self.scheduled_operations_count += 1;
    }
}

impl<T> Scheduler<T> for FairScheduler<T> {
    fn insert(&mut self, item: T, volume: f64, ctx: &mut SimulationContext) {
        if let Some(limit) = self.concurrent_operations_limit {
            if self.scheduled_operations_count >= limit {
                self.pending_operations.push_back((item, volume));
                return;
            }
        }
        self.schedule(item, volume, ctx);
    }

    fn pop(&mut self, ctx: &mut SimulationContext) -> Option<(f64, T)> {
        let result = self.throughput_model.pop();
        self.scheduled_operations_count -= 1;
        if result.is_some() {
            if let Some((item, volume)) = self.pending_operations.pop_front() {
                self.schedule(item, volume, ctx);
            }
        }
        result
    }

    fn peek(&self) -> Option<(f64, &T)> {
        self.throughput_model.peek()
    }
}
