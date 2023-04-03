use std::collections::{BinaryHeap, HashSet, VecDeque};

use rand::distributions::uniform::{SampleRange, SampleUniform};
use rand::distributions::{Alphanumeric, DistString};
use rand::prelude::*;
use rand_pcg::Pcg64;

use crate::component::Id;
use crate::event::{Event, EventData, EventId};
use crate::log::log_incorrect_event;

/// Epsilon to compare floating point values for equality.
pub const EPSILON: f64 = 1e-12;

pub struct SimulationState {
    clock: f64,
    rand: Pcg64,
    events: BinaryHeap<Event>,
    ordered_events: VecDeque<Event>,
    canceled_events: HashSet<EventId>,
    event_count: u64,
}

impl SimulationState {
    pub fn new(seed: u64) -> Self {
        Self {
            clock: 0.0,
            rand: Pcg64::seed_from_u64(seed),
            events: BinaryHeap::new(),
            ordered_events: VecDeque::new(),
            canceled_events: HashSet::new(),
            event_count: 0,
        }
    }

    pub fn time(&self) -> f64 {
        self.clock
    }

    pub fn set_time(&mut self, time: f64) {
        self.clock = time;
    }

    pub fn rand(&mut self) -> f64 {
        self.rand.gen_range(0.0..1.0)
    }

    pub fn gen_range<T, R>(&mut self, range: R) -> T
    where
        T: SampleUniform,
        R: SampleRange<T>,
    {
        self.rand.gen_range(range)
    }

    pub fn sample_from_distribution<T, Dist: Distribution<T>>(&mut self, dist: &Dist) -> T {
        dist.sample(&mut self.rand)
    }

    pub fn random_string(&mut self, len: usize) -> String {
        Alphanumeric.sample_string(&mut self.rand, len)
    }

    pub fn add_event<T>(&mut self, data: T, src: Id, dest: Id, delay: f64) -> EventId
    where
        T: EventData,
    {
        let event_id = self.event_count;
        let event = Event {
            id: event_id,
            time: self.clock + delay.max(0.),
            src,
            dest,
            data: Box::new(data),
        };
        if delay >= -EPSILON {
            self.events.push(event);
            self.event_count += 1;
            event_id
        } else {
            log_incorrect_event(event, &format!("negative delay {}", delay));
            panic!("Event delay is negative! It is not allowed to add events from the past.");
        }
    }

    pub fn add_ordered_event<T>(&mut self, data: T, src: Id, dest: Id, delay: f64) -> EventId
    where
        T: EventData,
    {
        if !self.can_add_ordered_event(delay) {
            panic!("Event order is broken! Ordered events should be added in non-decreasing order of their time.");
        }
        let last_time = self.ordered_events.back().map_or(f64::MIN, |x| x.time);
        let event_id = self.event_count;
        let event = Event {
            id: event_id,
            // max is used to enforce time order despite of floating-point errors
            time: last_time.max(self.clock + delay),
            src,
            dest,
            data: Box::new(data),
        };
        if delay >= 0. {
            self.ordered_events.push_back(event);
            self.event_count += 1;
            event_id
        } else {
            log_incorrect_event(event, &format!("negative delay {}", delay));
            panic!("Event delay is negative! It is not allowed to add events from the past.");
        }
    }

    pub fn can_add_ordered_event(&self, delay: f64) -> bool {
        if let Some(evt) = self.ordered_events.back() {
            // small epsilon is used to account for floating-point errors
            if delay + self.clock < evt.time - EPSILON {
                return false;
            }
        }
        true
    }

    pub fn next_event(&mut self) -> Option<Event> {
        loop {
            let maybe_heap = self.events.peek();
            let maybe_deque = self.ordered_events.front();
            if maybe_heap.is_some() && (maybe_deque.is_none() || maybe_heap.unwrap() > maybe_deque.unwrap()) {
                let event = self.events.pop().unwrap();
                if !self.canceled_events.remove(&event.id) {
                    self.clock = event.time;
                    return Some(event);
                }
            } else if maybe_deque.is_some() {
                let event = self.ordered_events.pop_front().unwrap();
                if !self.canceled_events.remove(&event.id) {
                    self.clock = event.time;
                    return Some(event);
                }
            } else {
                return None;
            }
        }
    }

    pub fn peek_event(&self) -> Option<&Event> {
        let maybe_heap = self.events.peek();
        let maybe_deque = self.ordered_events.front();
        if maybe_heap.is_some() && (maybe_deque.is_none() || maybe_heap.unwrap() > maybe_deque.unwrap()) {
            maybe_heap
        } else {
            maybe_deque
        }
    }

    pub fn cancel_event(&mut self, id: EventId) {
        self.canceled_events.insert(id);
    }

    pub fn cancel_events<F>(&mut self, pred: F)
    where
        F: Fn(&Event) -> bool,
    {
        for event in self.events.iter() {
            if pred(event) {
                self.canceled_events.insert(event.id);
            }
        }
        for event in self.ordered_events.iter() {
            if pred(event) {
                self.canceled_events.insert(event.id);
            }
        }
    }

    /// This function does not check events from `ordered_events`.
    pub fn cancel_heap_events<F>(&mut self, pred: F)
    where
        F: Fn(&Event) -> bool,
    {
        for event in self.events.iter() {
            if pred(event) {
                self.canceled_events.insert(event.id);
            }
        }
    }

    pub fn event_count(&self) -> u64 {
        self.event_count
    }
}
