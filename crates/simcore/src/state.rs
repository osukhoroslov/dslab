use std::collections::{BinaryHeap, HashSet};

use rand::distributions::uniform::{SampleRange, SampleUniform};
use rand::prelude::*;
use rand_pcg::Pcg64;

use crate::component::Id;
use crate::event::{Event, EventData, EventId};

pub struct SimulationState {
    clock: f64,
    rand: Pcg64,
    events: BinaryHeap<Event>,
    canceled_events: HashSet<EventId>,
    event_count: u64,
}

impl SimulationState {
    pub fn new(seed: u64) -> Self {
        Self {
            clock: 0.0,
            rand: Pcg64::seed_from_u64(seed),
            events: BinaryHeap::new(),
            canceled_events: HashSet::new(),
            event_count: 0,
        }
    }

    pub fn time(&self) -> f64 {
        self.clock
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

    pub fn add_event<T>(&mut self, data: T, src: Id, dest: Id, delay: f64) -> EventId
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
        self.events.peek()
    }

    pub fn cancel_event(&mut self, id: EventId) {
        self.canceled_events.insert(id);
    }

    pub fn event_count(&self) -> u64 {
        self.event_count
    }
}
