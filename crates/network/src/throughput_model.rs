use std::cmp::Ordering;
use std::collections::BinaryHeap;

struct ThroughputModelItem<T> {
    position: f64,
    id: u64,
    item: T,
}

impl<T> ThroughputModelItem<T> {
    fn new(position: f64, id: u64, item: T) -> Self {
        ThroughputModelItem { position, id, item }
    }
}

impl<T> PartialOrd for ThroughputModelItem<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for ThroughputModelItem<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .position
            .partial_cmp(&self.position)
            .unwrap()
            .then(self.id.cmp(&other.id))
    }
}

impl<T> PartialEq for ThroughputModelItem<T> {
    fn eq(&self, other: &Self) -> bool {
        self.position == other.position && self.id == other.id
    }
}

impl<T> Eq for ThroughputModelItem<T> {}

struct Line {
    k: f64,
    b: f64,
}

impl Line {
    fn new(k: f64, b: f64) -> Self {
        Line { k, b }
    }

    fn at(&self, x: f64) -> f64 {
        self.k * x + self.b
    }

    fn inv(&self) -> Line {
        Line::new(1. / self.k, -self.b / self.k)
    }

    fn then(&self, other: Line) -> Line {
        Line::new(self.k * other.k, self.b * other.k + other.b)
    }
}

pub struct ThroughputModel<T> {
    id: u64,
    throughput: f64,
    line: Line,
    items: BinaryHeap<ThroughputModelItem<T>>,
}

impl<T: std::fmt::Debug> ThroughputModel<T> {
    pub fn new(throughput: f64) -> Self {
        ThroughputModel {
            id: 0,
            throughput,
            line: Line::new(1., 0.),
            items: BinaryHeap::new(),
        }
    }

    pub fn insert(&mut self, current_time: f64, mut volume: f64, item: T) {
        volume /= self.throughput;
        self.id += 1;
        if self.items.is_empty() {
            self.line = Line::new(1., 0.);
            self.items
                .push(ThroughputModelItem::<T>::new(current_time + volume, self.id - 1, item));
        } else {
            let k = self.items.len() as f64;
            self.line = self.line.then(Line::new((k + 1.) / k, -1. / k * current_time));
            let length = volume * (k + 1.);
            self.items.push(ThroughputModelItem::<T>::new(
                self.line.inv().at(current_time + length),
                self.id - 1,
                item,
            ));
        }
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn pop(&mut self) -> Option<(f64, T)> {
        if let Some(item) = self.items.pop() {
            let k = self.items.len() as f64;
            let y = self.line.at(item.position);
            self.line = self.line.then(Line::new(k / (k + 1.), 1. / (k + 1.) * y));
            Some((y, item.item))
        } else {
            None
        }
    }

    pub fn peek(&mut self) -> Option<(f64, &T)> {
        self.items.peek().map(|x| (self.line.at(x.position), &x.item))
    }

    pub fn next_event(&self) -> Option<f64> {
        self.items.peek().map(|x| self.line.at(x.position))
    }
}
