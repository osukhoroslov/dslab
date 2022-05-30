use std::cmp::Ordering;
use std::collections::BinaryHeap;

use sugars::boxed;

use simcore::component::Fractional;

use crate::model::*;

struct FairThroughputSharingModelEntry<T> {
    position: Fractional,
    id: u64,
    item: T,
}

impl<T> FairThroughputSharingModelEntry<T> {
    fn new(position: Fractional, id: u64, item: T) -> Self {
        Self { position, id, item }
    }
}

impl<T> PartialOrd for FairThroughputSharingModelEntry<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for FairThroughputSharingModelEntry<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .position
            .partial_cmp(&self.position)
            .unwrap()
            .then(other.id.cmp(&self.id))
    }
}

impl<T> PartialEq for FairThroughputSharingModelEntry<T> {
    fn eq(&self, other: &Self) -> bool {
        self.position == other.position && self.id == other.id
    }
}

impl<T> Eq for FairThroughputSharingModelEntry<T> {}

struct TimeFunction {
    k: Fractional,
    b: Fractional,
}

impl TimeFunction {
    fn ident() -> Self {
        Self {
            k: Fractional::one(),
            b: Fractional::zero(),
        }
    }

    fn at(&self, time: Fractional) -> Fractional {
        self.k * time + self.b
    }

    fn inversed(&self) -> Self {
        Self {
            k: Fractional::one() / self.k,
            b: -self.b / self.k,
        }
    }

    fn update(&mut self, current_time: Fractional, throughput_ratio: Fractional) {
        self.k *= throughput_ratio;
        self.b = self.b * throughput_ratio + current_time * (Fractional::one() - throughput_ratio);
    }
}

pub struct FairThroughputSharingModel<T> {
    throughput_function: Box<dyn Fn(usize) -> Fractional>,
    time_fn: TimeFunction,
    entries: BinaryHeap<FairThroughputSharingModelEntry<T>>,
    next_id: u64,
    last_throughput_per_item: Fractional,
}

impl<T> FairThroughputSharingModel<T> {
    pub fn with_fixed_throughput(throughput: Fractional) -> Self {
        Self::with_dynamic_throughput(boxed!(move |_| throughput))
    }

    pub fn with_dynamic_throughput(throughput_function: Box<dyn Fn(usize) -> Fractional>) -> Self {
        Self {
            throughput_function,
            time_fn: TimeFunction::ident(),
            entries: BinaryHeap::new(),
            next_id: 0,
            last_throughput_per_item: Fractional::zero(),
        }
    }
}

impl<T> ThroughputModel<T> for FairThroughputSharingModel<T> {
    fn insert(&mut self, current_time: Fractional, volume: Fractional, item: T) {
        if self.entries.is_empty() {
            self.last_throughput_per_item = (self.throughput_function)(1);
            let finish_time = current_time + volume / self.last_throughput_per_item;
            self.time_fn = TimeFunction::ident();
            self.entries.push(FairThroughputSharingModelEntry::<T>::new(
                finish_time,
                self.next_id,
                item,
            ));
        } else {
            let new_count = self.entries.len() + 1;
            let new_throughput_per_item =
                (self.throughput_function)(new_count) / Fractional::from_integer(new_count.try_into().unwrap());
            self.time_fn
                .update(current_time, self.last_throughput_per_item / new_throughput_per_item);
            self.last_throughput_per_item = new_throughput_per_item;
            let finish_time = current_time + volume / new_throughput_per_item;
            self.entries.push(FairThroughputSharingModelEntry::<T>::new(
                self.time_fn.inversed().at(finish_time),
                self.next_id,
                item,
            ));
        }
        self.next_id += 1;
    }

    fn pop(&mut self) -> Option<(Fractional, T)> {
        if let Some(entry) = self.entries.pop() {
            let current_time = self.time_fn.at(entry.position);
            let new_count = self.entries.len();
            if new_count > 0 {
                let new_throughput_per_item =
                    (self.throughput_function)(new_count) / Fractional::from_integer(new_count.try_into().unwrap());
                self.time_fn
                    .update(current_time, self.last_throughput_per_item / new_throughput_per_item);
                self.last_throughput_per_item = new_throughput_per_item;
            } else {
                self.time_fn = TimeFunction::ident();
                self.last_throughput_per_item = Fractional::zero();
            }
            return Some((current_time, entry.item));
        }
        None
    }

    fn next_time(&self) -> Option<(Fractional, &T)> {
        self.entries
            .peek()
            .map(|entry| (self.time_fn.at(entry.position), &entry.item))
    }
}
