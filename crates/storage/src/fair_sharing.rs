use std::cmp::Ordering;
use std::collections::BinaryHeap;

struct FairThroughputSharingModelItem<T> {
    position: f64,
    id: u64,
    item: T,
}

impl<T> FairThroughputSharingModelItem<T> {
    fn new(position: f64, id: u64, item: T) -> Self {
        FairThroughputSharingModelItem { position, id, item }
    }
}

impl<T> PartialOrd for FairThroughputSharingModelItem<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for FairThroughputSharingModelItem<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .position
            .partial_cmp(&self.position)
            .unwrap()
            .then(other.id.cmp(&self.id))
    }
}

impl<T> PartialEq for FairThroughputSharingModelItem<T> {
    fn eq(&self, other: &Self) -> bool {
        self.position == other.position && self.id == other.id
    }
}

impl<T> Eq for FairThroughputSharingModelItem<T> {}

struct TimeFunction {
    a: f64,
    b: f64,
}

impl TimeFunction {
    fn new(a: f64, b: f64) -> Self {
        TimeFunction { a, b }
    }

    fn at(&self, x: f64) -> f64 {
        self.a * x + self.b
    }

    fn inverse(&self) -> TimeFunction {
        TimeFunction::new(1. / self.a, -self.b / self.a)
    }

    fn update(&mut self, c1: f64, c2: f64) {
        self.a *= c1;
        self.b = self.b * c1 + c2;
    }
}

pub struct FairThroughputSharingModel<T> {
    throughput: f64,
    time_fn: TimeFunction,
    items: BinaryHeap<FairThroughputSharingModelItem<T>>,
    next_id: u64,
}

impl<T> FairThroughputSharingModel<T> {
    pub fn new(throughput: f64) -> Self {
        FairThroughputSharingModel {
            throughput,
            time_fn: TimeFunction::new(1., 0.),
            items: BinaryHeap::new(),
            next_id: 0,
        }
    }

    pub fn insert(&mut self, current_time: f64, volume: f64, item: T) {
        if self.items.is_empty() {
            let finish_time = current_time + volume / self.throughput;
            self.time_fn = TimeFunction::new(1., 0.);
            self.items.push(FairThroughputSharingModelItem::<T>::new(
                finish_time,
                self.next_id,
                item,
            ));
        } else {
            let par_old = self.items.len() as f64;
            let par_new = par_old + 1.;
            self.time_fn.update(par_new / par_old, -current_time / par_old);
            let finish_time = current_time + (volume / self.throughput) * par_new;
            self.items.push(FairThroughputSharingModelItem::<T>::new(
                self.time_fn.inverse().at(finish_time),
                self.next_id,
                item,
            ));
        }
        self.next_id += 1;
    }

    pub fn pop(&mut self) -> Option<(f64, T)> {
        if let Some(item) = self.items.pop() {
            let par_new = self.items.len() as f64;
            let par_old = par_new + 1.;
            let current_time = self.time_fn.at(item.position);
            self.time_fn.update(par_new / par_old, current_time / par_old);
            Some((current_time, item.item))
        } else {
            None
        }
    }

    pub fn next_time(&self) -> Option<f64> {
        self.items.peek().map(|x| self.time_fn.at(x.position))
    }
}
