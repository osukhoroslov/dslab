//! Slow implementation of fair throughput sharing model, which recalculates all event times
//! at each activity creation and completion.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use sugars::boxed;

use dslab_core::SimulationContext;

use super::functions::{make_constant_throughput_fn, ConstantFactorFn};
use super::model::{ActivityFactorFn, ActivityId, ResourceThroughputFn, ThroughputSharingModel};

struct Activity<T> {
    remaining_volume: f64,
    id: ActivityId,
    item: T,
}

impl<T> Activity<T> {
    fn new(remaining_volume: f64, id: u64, item: T) -> Self {
        Self {
            remaining_volume,
            id,
            item,
        }
    }
}

impl<T> PartialOrd for Activity<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for Activity<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .remaining_volume
            .total_cmp(&self.remaining_volume)
            .then(other.id.cmp(&self.id))
    }
}

impl<T> PartialEq for Activity<T> {
    fn eq(&self, other: &Self) -> bool {
        self.remaining_volume == other.remaining_volume && self.id == other.id
    }
}

impl<T> Eq for Activity<T> {}

/// Slow implementation of fair throughput sharing model, which recalculates all event times at each activity creation
/// and completion.
pub struct SlowFairThroughputSharingModel<T> {
    throughput_function: ResourceThroughputFn,
    factor_function: Box<dyn ActivityFactorFn<T>>,
    entries: BinaryHeap<Activity<T>>,
    next_id: u64,
    last_throughput_per_item: f64,
    last_recalculation_time: f64,
}

impl<T> SlowFairThroughputSharingModel<T> {
    /// Creates model with given throughput and factor functions.
    pub fn new(throughput_function: ResourceThroughputFn, factor_function: Box<dyn ActivityFactorFn<T>>) -> Self {
        Self {
            throughput_function,
            factor_function,
            entries: BinaryHeap::new(),
            next_id: 0,
            last_throughput_per_item: 0.,
            last_recalculation_time: 0.,
        }
    }

    /// Creates model with fixed throughput.
    pub fn with_fixed_throughput(throughput: f64) -> Self {
        Self::with_dynamic_throughput(make_constant_throughput_fn(throughput))
    }

    /// Creates model with dynamic throughput, represented by given closure.
    pub fn with_dynamic_throughput(throughput_function: ResourceThroughputFn) -> Self {
        Self {
            throughput_function,
            factor_function: boxed!(ConstantFactorFn::new(1.)),
            entries: BinaryHeap::new(),
            next_id: 0,
            last_throughput_per_item: 0.,
            last_recalculation_time: 0.,
        }
    }

    fn recalculate(&mut self, current_time: f64, throughput_per_item: f64) {
        let mut new_entries = BinaryHeap::<Activity<T>>::with_capacity(self.entries.len());
        let processed_volume = (current_time - self.last_recalculation_time) * self.last_throughput_per_item;
        while let Some(entry) = self.entries.pop() {
            let remaining_volume = entry.remaining_volume - processed_volume;
            new_entries.push(Activity::<T>::new(remaining_volume, entry.id, entry.item));
        }
        self.entries = new_entries;
        self.last_throughput_per_item = throughput_per_item;
        self.last_recalculation_time = current_time;
    }
}

impl<T> ThroughputSharingModel<T> for SlowFairThroughputSharingModel<T> {
    fn insert(&mut self, item: T, volume: f64, ctx: &SimulationContext) -> ActivityId {
        let new_count = self.entries.len() + 1;
        self.recalculate(ctx.time(), (self.throughput_function)(new_count) / new_count as f64);
        let volume = volume / self.factor_function.get_factor(&item, ctx);
        let next_id = self.next_id;
        self.entries.push(Activity::<T>::new(volume, next_id, item));
        self.next_id += 1;
        next_id
    }

    fn pop(&mut self) -> Option<(f64, T)> {
        if let Some(entry) = self.entries.pop() {
            let complete_time = self.last_recalculation_time + entry.remaining_volume / self.last_throughput_per_item;
            if self.entries.is_empty() {
                self.last_recalculation_time = complete_time;
                self.last_throughput_per_item = 0.;
            } else {
                let new_count = self.entries.len();
                self.recalculate(complete_time, (self.throughput_function)(new_count) / new_count as f64);
            }
            return Some((complete_time, entry.item));
        }
        None
    }

    fn peek(&mut self) -> Option<(f64, &T)> {
        self.entries.peek().map(|entry| {
            (
                self.last_recalculation_time + entry.remaining_volume / self.last_throughput_per_item,
                &entry.item,
            )
        })
    }
}
