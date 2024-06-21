//! Fast implementation of fair throughput sharing model with ability to cancel activities.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use rustc_hash::FxHashMap;
use sugars::boxed;

use dslab_core::SimulationContext;

use super::functions::{make_constant_throughput_fn, ConstantFactorFn};
use super::model::{ActivityFactorFn, ActivityId, ResourceThroughputFn, ThroughputSharingModel};

const TOTAL_WORK_MAX_VALUE: f64 = 1e12;

struct Activity<T> {
    start_work: f64,
    item: T,
}

impl<T> Activity<T> {
    fn new(start_work: f64, item: T) -> Self {
        Self { start_work, item }
    }
}

#[derive(Clone, Copy)]
struct ActivityInfo {
    id: ActivityId,
    finish_work: f64,
}

impl ActivityInfo {
    fn new(id: ActivityId, finish_work: f64) -> Self {
        Self { id, finish_work }
    }
}

impl PartialOrd for ActivityInfo {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ActivityInfo {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .finish_work
            .total_cmp(&self.finish_work)
            .then(other.id.cmp(&self.id))
    }
}

impl PartialEq for ActivityInfo {
    fn eq(&self, other: &Self) -> bool {
        self.finish_work == other.finish_work && self.id == other.id
    }
}

impl Eq for ActivityInfo {}

/// Fast implementation of fair throughput sharing model with ability to cancel activities.
///
/// According to our tests, it is slightly (10-20%) slower than
/// [`FairThroughputSharingModel`](crate::throughput_sharing::FairThroughputSharingModel).
pub struct FairThroughputSharingModelWithCancel<T> {
    activities_queue: BinaryHeap<ActivityInfo>,
    running_activities: FxHashMap<ActivityId, Activity<T>>,
    throughput_function: ResourceThroughputFn,
    factor_function: Box<dyn ActivityFactorFn<T>>,
    throughput_per_activity: f64,
    next_id: u64,
    total_work: f64,
    last_update: f64,
}

impl<T> FairThroughputSharingModelWithCancel<T> {
    /// Creates model with given throughput and factor functions.
    pub fn new(throughput_function: ResourceThroughputFn, factor_function: Box<dyn ActivityFactorFn<T>>) -> Self {
        Self {
            activities_queue: BinaryHeap::new(),
            running_activities: FxHashMap::default(),
            throughput_function,
            factor_function,
            throughput_per_activity: 0.,
            next_id: 0,
            total_work: 0.,
            last_update: 0.,
        }
    }

    /// Creates model with fixed throughput.
    pub fn with_fixed_throughput(throughput: f64) -> Self {
        Self::with_dynamic_throughput(make_constant_throughput_fn(throughput))
    }

    /// Creates model with dynamic throughput, represented by given closure.
    pub fn with_dynamic_throughput(throughput_function: ResourceThroughputFn) -> Self {
        Self {
            activities_queue: BinaryHeap::new(),
            running_activities: FxHashMap::default(),
            throughput_function,
            factor_function: boxed!(ConstantFactorFn::new(1.)),
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
            while !self.activities_queue.is_empty() {
                let mut activity = self.activities_queue.pop().unwrap();
                activity.finish_work -= self.total_work;
                entries_vec.push(activity);
            }
            self.activities_queue = entries_vec.into();
            self.running_activities.iter_mut().for_each(|(_id, a)| {
                a.start_work -= self.total_work;
            });
            self.total_work = 0.;
        }
    }

    fn recalculate_throughput(&mut self) {
        let count = self.running_activities.len();
        if count > 0 {
            self.throughput_per_activity = (self.throughput_function)(count) / count as f64;
        } else {
            self.throughput_per_activity = 0.;
        }
    }
}

impl<T> ThroughputSharingModel<T> for FairThroughputSharingModelWithCancel<T> {
    fn insert(&mut self, item: T, volume: f64, ctx: &SimulationContext) -> ActivityId {
        if !self.activities_queue.is_empty() {
            self.increment_total_work((ctx.time() - self.last_update) * self.throughput_per_activity);
        }
        let volume = volume / self.factor_function.get_factor(&item, ctx);
        let finish_work = self.total_work + volume;
        let next_id = self.next_id;
        let activity_info = ActivityInfo::new(next_id, finish_work);
        self.activities_queue.push(activity_info);
        self.running_activities
            .insert(next_id, Activity::<T>::new(self.total_work, item));
        self.next_id += 1;
        self.recalculate_throughput();
        self.last_update = ctx.time();
        next_id
    }

    fn pop(&mut self) -> Option<(f64, T)> {
        while let Some(entry) = self.activities_queue.pop() {
            if let Some(activity) = self.running_activities.remove(&entry.id) {
                let remaining_work = entry.finish_work - self.total_work;
                let finish_time = self.last_update + remaining_work / self.throughput_per_activity;
                self.increment_total_work(remaining_work);
                self.recalculate_throughput();
                self.last_update = finish_time;
                return Some((finish_time, activity.item));
            }
        }
        None
    }

    fn cancel(&mut self, id: ActivityId, ctx: &SimulationContext) -> Option<(f64, T)> {
        if let Some(activity) = self.running_activities.remove(&id) {
            if !self.activities_queue.is_empty() {
                self.increment_total_work((ctx.time() - self.last_update) * self.throughput_per_activity);
            }

            self.recalculate_throughput();
            self.last_update = ctx.time();

            let volume_done = self.total_work - activity.start_work;
            Some((volume_done, activity.item))
        } else {
            None
        }
    }

    fn peek(&mut self) -> Option<(f64, &T)> {
        while let Some(entry) = self.activities_queue.peek() {
            if let Some(activity) = self.running_activities.get(&entry.id) {
                return Some((
                    self.last_update + (entry.finish_work - self.total_work) / self.throughput_per_activity,
                    &activity.item,
                ));
            } else {
                self.activities_queue.pop();
            }
        }
        None
    }
}
