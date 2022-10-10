use std::cmp::Ordering;
use std::collections::BinaryHeap;

use sugars::boxed;

use dslab_models::throughput_sharing::ThroughputSharingModel;

struct Activity<T> {
    position: f64,
    id: u64,
    item: T,
}

impl<T> Activity<T> {
    fn new(position: f64, id: u64, item: T) -> Self {
        Self { position, id, item }
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
            .position
            .partial_cmp(&self.position)
            .unwrap()
            .then(other.id.cmp(&self.id))
    }
}

impl<T> PartialEq for Activity<T> {
    fn eq(&self, other: &Self) -> bool {
        self.position == other.position && self.id == other.id
    }
}

impl<T> Eq for Activity<T> {}

// Linear time function y = a * x + b
struct TimeFunction {
    a: f64,
    b: f64,
}

impl TimeFunction {
    fn ident() -> Self {
        Self { a: 1., b: 0. }
    }

    fn at(&self, time: f64) -> f64 {
        self.a * time + self.b
    }

    fn inversed(&self) -> Self {
        Self {
            a: 1. / self.a,
            b: -self.b / self.a,
        }
    }

    fn update(&mut self, current_time: f64, throughput_ratio: f64) {
        self.a *= throughput_ratio;
        self.b = self.b * throughput_ratio + current_time * (1. - throughput_ratio);
    }
}

pub struct FairThroughputSharingModel<T> {
    throughput_function: Box<dyn Fn(usize) -> f64>,
    time_fn: TimeFunction,
    entries: BinaryHeap<Activity<T>>,
    next_id: u64,
    last_throughput_per_item: f64,
}

impl<T> FairThroughputSharingModel<T> {
    pub fn with_fixed_throughput(throughput: f64) -> Self {
        Self::with_dynamic_throughput(boxed!(move |_| throughput))
    }

    pub fn with_dynamic_throughput(throughput_function: Box<dyn Fn(usize) -> f64>) -> Self {
        Self {
            throughput_function,
            time_fn: TimeFunction::ident(),
            entries: BinaryHeap::new(),
            next_id: 0,
            last_throughput_per_item: 0.,
        }
    }
}

impl<T> ThroughputSharingModel<T> for FairThroughputSharingModel<T> {
    fn insert(&mut self, current_time: f64, volume: f64, item: T) {
        if self.entries.is_empty() {
            self.last_throughput_per_item = (self.throughput_function)(1);
            let finish_time = current_time + volume / self.last_throughput_per_item;
            self.time_fn = TimeFunction::ident();
            self.entries.push(Activity::<T>::new(finish_time, self.next_id, item));
        } else {
            let new_count = self.entries.len() + 1;
            let new_throughput_per_item = (self.throughput_function)(new_count) / new_count as f64;
            self.time_fn
                .update(current_time, self.last_throughput_per_item / new_throughput_per_item);
            self.last_throughput_per_item = new_throughput_per_item;
            let finish_time = current_time + volume / new_throughput_per_item;
            self.entries.push(Activity::<T>::new(
                self.time_fn.inversed().at(finish_time),
                self.next_id,
                item,
            ));
        }
        self.next_id += 1;
    }

    fn pop(&mut self) -> Option<(f64, T)> {
        if let Some(entry) = self.entries.pop() {
            let current_time = self.time_fn.at(entry.position);
            let new_count = self.entries.len();
            if new_count > 0 {
                let new_throughput_per_item = (self.throughput_function)(new_count) / new_count as f64;
                self.time_fn
                    .update(current_time, self.last_throughput_per_item / new_throughput_per_item);
                self.last_throughput_per_item = new_throughput_per_item;
            } else {
                self.time_fn = TimeFunction::ident();
                self.last_throughput_per_item = 0.;
            }
            return Some((current_time, entry.item));
        }
        None
    }

    fn peek(&self) -> Option<(f64, &T)> {
        self.entries
            .peek()
            .map(|entry| (self.time_fn.at(entry.position), &entry.item))
    }
}
