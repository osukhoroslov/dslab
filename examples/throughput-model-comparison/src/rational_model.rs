use std::cmp::Ordering;
use std::collections::BinaryHeap;

use num::bigint::Sign;
use num::rational::BigRational;
use num::BigInt;

fn one() -> BigInt {
    BigInt::new(Sign::Plus, vec![1])
}
fn zero() -> BigInt {
    BigInt::new(Sign::Plus, vec![0])
}

struct Activity<T> {
    position: BigRational,
    id: u64,
    item: T,
}

impl<T> Activity<T> {
    fn new(position: BigRational, id: u64, item: T) -> Self {
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
        other.position.cmp(&self.position).then(other.id.cmp(&self.id))
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
    a: BigRational,
    b: BigRational,
}

impl TimeFunction {
    fn ident() -> Self {
        Self {
            a: BigRational::new(one(), one()),
            b: BigRational::new(zero(), one()),
        }
    }

    fn at(&self, time: BigRational) -> BigRational {
        self.a.clone() * time + self.b.clone()
    }

    fn inversed(&self) -> Self {
        Self {
            a: BigRational::new(one(), one()) / self.a.clone(),
            b: -self.b.clone() / self.a.clone(),
        }
    }

    fn update(&mut self, current_time: BigRational, throughput_ratio: BigRational) {
        self.a *= throughput_ratio.clone();
        self.b = self.b.clone() * throughput_ratio.clone()
            + current_time * (BigRational::new(one(), one()) - throughput_ratio);
    }
}

pub struct FairThroughputSharingModelRational<T> {
    throughput: BigRational,
    time_fn: TimeFunction,
    entries: BinaryHeap<Activity<T>>,
    next_id: u64,
    last_throughput_per_item: BigRational,
}

impl<T> FairThroughputSharingModelRational<T> {
    pub fn new(throughput: BigRational) -> Self {
        Self {
            throughput,
            time_fn: TimeFunction::ident(),
            entries: BinaryHeap::new(),
            next_id: 0,
            last_throughput_per_item: BigRational::new(zero(), one()),
        }
    }
}

impl<T> FairThroughputSharingModelRational<T> {
    pub fn insert(&mut self, current_time: BigRational, volume: BigRational, item: T) {
        if self.entries.is_empty() {
            self.last_throughput_per_item = self.throughput.clone();
            let finish_time = current_time + volume / self.last_throughput_per_item.clone();
            self.time_fn = TimeFunction::ident();
            self.entries.push(Activity::<T>::new(finish_time, self.next_id, item));
        } else {
            let new_count = self.entries.len() + 1;
            let new_throughput_per_item = self.throughput.clone() / BigInt::new(Sign::Plus, vec![new_count as u32]);
            self.time_fn.update(
                current_time.clone(),
                self.last_throughput_per_item.clone() / new_throughput_per_item.clone(),
            );
            self.last_throughput_per_item = new_throughput_per_item.clone();
            let finish_time = current_time + volume / new_throughput_per_item;
            self.entries.push(Activity::<T>::new(
                self.time_fn.inversed().at(finish_time),
                self.next_id,
                item,
            ));
        }
        self.next_id += 1;
    }

    pub fn pop(&mut self) -> Option<(BigRational, T)> {
        if let Some(entry) = self.entries.pop() {
            let current_time = self.time_fn.at(entry.position);
            let new_count = self.entries.len();
            if new_count > 0 {
                let new_throughput_per_item = self.throughput.clone() / BigInt::new(Sign::Plus, vec![new_count as u32]);
                self.time_fn.update(
                    current_time.clone(),
                    self.last_throughput_per_item.clone() / new_throughput_per_item.clone(),
                );
                self.last_throughput_per_item = new_throughput_per_item;
            } else {
                self.time_fn = TimeFunction::ident();
                self.last_throughput_per_item = BigRational::new(zero(), one());
            }
            return Some((current_time, entry.item));
        }
        None
    }

    pub fn peek(&self) -> Option<(BigRational, &T)> {
        self.entries
            .peek()
            .map(|entry| (self.time_fn.at(entry.position.clone()), &entry.item))
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }
}
