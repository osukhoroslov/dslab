//! Fast implementation of fair throughput sharing model.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use sugars::boxed;

use super::model::{ThroughputFunction, ThroughputSharingModel};

const TOTAL_WORK_MAX_VALUE: f64 = 1e12;

struct Activity<T> {
    id: u64,
    item: T,
    finish_work: f64,
}

impl<T> Activity<T> {
    fn new(id: u64, item: T, finish_work: f64) -> Self {
        Self { id, item, finish_work }
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
            .finish_work
            .total_cmp(&self.finish_work)
            .then(other.id.cmp(&self.id))
    }
}

impl<T> PartialEq for Activity<T> {
    fn eq(&self, other: &Self) -> bool {
        self.finish_work == other.finish_work && self.id == other.id
    }
}

impl<T> Eq for Activity<T> {}

/// Fast implementation of fair throughput sharing model.
pub struct FairThroughputSharingModel<T> {
    activities: BinaryHeap<Activity<T>>,
    throughput_function: ThroughputFunction,
    throughput_per_activity: f64,
    next_id: u64,
    total_work: f64,
    last_update: f64,
}

impl<T> FairThroughputSharingModel<T> {
    /// Creates model with fixed throughput.
    pub fn with_fixed_throughput(throughput: f64) -> Self {
        Self::with_dynamic_throughput(boxed!(move |_| throughput))
    }

    /// Creates model with dynamic throughput, represented by given closure.
    pub fn with_dynamic_throughput(throughput_function: ThroughputFunction) -> Self {
        Self {
            activities: BinaryHeap::new(),
            throughput_function,
            throughput_per_activity: 0.,
            next_id: 0,
            total_work: 0.,
            last_update: 0.,
        }
    }

    fn increment_total_work(&mut self, delta: f64) {
        self.total_work += delta;
        if self.total_work > TOTAL_WORK_MAX_VALUE {
            let mut entries_vec = Vec::new();
            while !self.activities.is_empty() {
                let mut activity = self.activities.pop().unwrap();
                activity.finish_work -= self.total_work;
                entries_vec.push(activity);
            }
            self.activities = entries_vec.into();
            self.total_work = 0.;
        }
    }
}

impl<T> ThroughputSharingModel<T> for FairThroughputSharingModel<T> {
    fn insert(&mut self, current_time: f64, volume: f64, item: T) {
        if !self.activities.is_empty() {
            self.increment_total_work((current_time - self.last_update) * self.throughput_per_activity);
        }
        let finish_work = self.total_work + volume;
        self.activities
            .push(Activity::<T>::new(self.next_id, item, finish_work));
        self.next_id += 1;
        let count = self.activities.len();
        self.throughput_per_activity = (self.throughput_function)(count) / count as f64;
        self.last_update = current_time;
    }

    fn pop(&mut self) -> Option<(f64, T)> {
        if let Some(entry) = self.activities.pop() {
            let remaining_work = entry.finish_work - self.total_work;
            let finish_time = self.last_update + remaining_work / self.throughput_per_activity;
            self.increment_total_work(remaining_work);
            let count = self.activities.len();
            if count > 0 {
                self.throughput_per_activity = (self.throughput_function)(count) / count as f64;
            } else {
                self.throughput_per_activity = 0.;
            }
            self.last_update = finish_time;
            return Some((finish_time, entry.item));
        }
        None
    }

    fn peek(&self) -> Option<(f64, &T)> {
        self.activities.peek().map(|entry| {
            (
                self.last_update + (entry.finish_work - self.total_work) / self.throughput_per_activity,
                &entry.item,
            )
        })
    }
}
