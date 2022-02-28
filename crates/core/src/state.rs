use std::collections::{BinaryHeap, HashSet};

use decorum::R64;
use rand::distributions::uniform::{SampleRange, SampleUniform};
use rand::prelude::*;
use rand_pcg::Pcg64;

use crate::event::{Event, EventData};

pub struct SimulationState {
    clock: R64,
    rand: Pcg64,
    events: BinaryHeap<Event>,
    canceled_events: HashSet<u64>,
    event_count: u64,
}

impl SimulationState {
    pub fn new(seed: u64) -> Self {
        Self {
            clock: R64::from_inner(0.0),
            rand: Pcg64::seed_from_u64(seed),
            events: BinaryHeap::new(),
            canceled_events: HashSet::new(),
            event_count: 0,
        }
    }

    pub fn time(&self) -> f64 {
        self.clock.into_inner()
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

    pub fn add_event<T>(&mut self, data: T, src: String, dest: String, delay: f64) -> u64
    where
        T: EventData,
    {
        let event_id = self.event_count;
        let event = Event {
            id: event_id,
            time: self.clock + delay,
            src,
            dest,
            data: Box::new(data),
        };
        self.events.push(event);
        self.event_count += 1;
        event_id
    }

    pub fn next_event(&mut self) -> Option<Event> {
        loop {
            if let Some(event) = self.events.pop() {
                self.clock = event.time;
                if !self.canceled_events.remove(&event.id) {
                    return Some(event);
                }
            } else {
                return None;
            }
        }
    }

    pub fn peek_event(&self) -> Option<&Event> {
        self.events.peek()
    }

    pub fn cancel_event(&mut self, event_id: u64) {
        self.canceled_events.insert(event_id);
    }

    pub fn event_count(&self) -> u64 {
        self.event_count
    }
}
