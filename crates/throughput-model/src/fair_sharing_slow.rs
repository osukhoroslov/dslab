use std::cmp::Ordering;
use std::collections::BinaryHeap;

use sugars::boxed;

use simcore::component::Fractional;

use crate::model::ThroughputModel;

struct FairThroughputSharingSlowModelEntry<T> {
    remaining_volume: Fractional,
    id: u64,
    item: T,
}

impl<T> FairThroughputSharingSlowModelEntry<T> {
    fn new(remaining_volume: Fractional, id: u64, item: T) -> Self {
        Self {
            remaining_volume,
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
    throughput_function: Box<dyn Fn(usize) -> Fractional>,
    entries: BinaryHeap<FairThroughputSharingSlowModelEntry<T>>,
    next_id: u64,
    last_throughput_per_item: Fractional,
    last_recalculation_time: Fractional,
}

impl<T> FairThroughputSharingSlowModel<T> {
    pub fn with_fixed_throughput(throughput: Fractional) -> Self {
        Self::with_dynamic_throughput(boxed!(move |_| throughput))
    }

    pub fn with_dynamic_throughput(throughput_function: Box<dyn Fn(usize) -> Fractional>) -> Self {
        Self {
            throughput_function,
            entries: BinaryHeap::new(),
            next_id: 0,
            last_throughput_per_item: Fractional::zero(),
            last_recalculation_time: Fractional::zero(),
        }
    }

    fn recalculate(&mut self, current_time: Fractional, throughput_per_item: Fractional) {
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
    fn insert(&mut self, current_time: Fractional, volume: Fractional, item: T) {
        let new_count = self.entries.len() + 1;
        self.recalculate(
            current_time,
            (self.throughput_function)(new_count) / Fractional::from_integer(new_count.try_into().unwrap()),
        );
        self.entries.push(FairThroughputSharingSlowModelEntry::<T>::new(
            volume,
            self.next_id,
            item,
        ));
        self.next_id += 1;
    }

    fn pop(&mut self) -> Option<(Fractional, T)> {
        if let Some(entry) = self.entries.pop() {
            let complete_time = self.last_recalculation_time + entry.remaining_volume / self.last_throughput_per_item;
            if self.entries.is_empty() {
                self.last_recalculation_time = complete_time;
                self.last_throughput_per_item = Fractional::zero();
            } else {
                let new_count = self.entries.len();
                self.recalculate(
                    complete_time,
                    (self.throughput_function)(new_count) / Fractional::from_integer(new_count.try_into().unwrap()),
                );
            }
            return Some((complete_time, entry.item));
        }
        None
    }

    fn next_time(&self) -> Option<(Fractional, &T)> {
        self.entries.peek().map(|entry| {
            (
                self.last_recalculation_time + entry.remaining_volume / self.last_throughput_per_item,
                &entry.item,
            )
        })
    }
}
