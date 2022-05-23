use std::cmp::Ordering;
use std::collections::BinaryHeap;

use sugars::boxed;

use crate::model::ThroughputModel;

struct FairThroughputSharingSlowModelEntry<T> {
    remaining_volume: f64,
    id: u64,
    item: T,
}

impl<T> FairThroughputSharingSlowModelEntry<T> {
    fn new(remaining_size: f64, id: u64, item: T) -> Self {
        FairThroughputSharingSlowModelEntry {
            remaining_volume: remaining_size,
            id,
            item,
        }
    }
}

impl<T> PartialOrd for FairThroughputSharingSlowModelEntry<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for FairThroughputSharingSlowModelEntry<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .remaining_volume
            .partial_cmp(&self.remaining_volume)
            .unwrap()
            .then(other.id.cmp(&self.id))
    }
}

impl<T> PartialEq for FairThroughputSharingSlowModelEntry<T> {
    fn eq(&self, other: &Self) -> bool {
        self.remaining_volume == other.remaining_volume && self.id == other.id
    }
}

impl<T> Eq for FairThroughputSharingSlowModelEntry<T> {}

pub struct FairThroughputSharingSlowModel<T> {
    throughput_function: Box<dyn Fn(usize) -> f64>,
    entries: BinaryHeap<FairThroughputSharingSlowModelEntry<T>>,
    next_id: u64,
    last_throughput_per_item: f64,
    last_recalculation_time: f64,
}

impl<T> FairThroughputSharingSlowModel<T> {
    pub fn with_fixed_throughput(throughput: f64) -> Self {
        Self::with_dynamic_throughput(boxed!(move |n| throughput / n as f64))
    }

    pub fn with_dynamic_throughput(throughput_function: Box<dyn Fn(usize) -> f64>) -> Self {
        Self {
            throughput_function,
            entries: BinaryHeap::new(),
            next_id: 0,
            last_throughput_per_item: 0.,
            last_recalculation_time: 0.,
        }
    }

    fn recalculate(&mut self, current_time: f64, throughput_per_item: f64) {
        let mut new_entries = BinaryHeap::<FairThroughputSharingSlowModelEntry<T>>::with_capacity(self.entries.len());
        let processed_volume = (current_time - self.last_recalculation_time) * self.last_throughput_per_item;
        while let Some(entry) = self.entries.pop() {
            let remaining_volume = entry.remaining_volume - processed_volume;
            new_entries.push(FairThroughputSharingSlowModelEntry::<T>::new(
                remaining_volume,
                entry.id,
                entry.item,
            ));
        }
        self.entries = new_entries;
        self.last_throughput_per_item = throughput_per_item;
        self.last_recalculation_time = current_time;
    }
}

impl<T> ThroughputModel<T> for FairThroughputSharingSlowModel<T> {
    fn insert(&mut self, current_time: f64, volume: f64, item: T) {
        self.recalculate(current_time, (self.throughput_function)(self.entries.len() + 1));
        self.entries.push(FairThroughputSharingSlowModelEntry::<T>::new(
            volume,
            self.next_id,
            item,
        ));
        self.next_id += 1;
    }

    fn pop(&mut self) -> Option<(f64, T)> {
        if let Some(entry) = self.entries.pop() {
            let complete_time = self.last_recalculation_time + entry.remaining_volume / self.last_throughput_per_item;
            if self.entries.is_empty() {
                self.last_recalculation_time = complete_time;
                self.last_throughput_per_item = 0.;
            } else {
                self.recalculate(complete_time, (self.throughput_function)(self.entries.len()));
            }
            return Some((complete_time, entry.item));
        }
        None
    }

    fn next_time(&self) -> Option<(f64, &T)> {
        self.entries.peek().map(|entry| {
            (
                self.last_recalculation_time + entry.remaining_volume / self.last_throughput_per_item,
                &entry.item,
            )
        })
    }
}
