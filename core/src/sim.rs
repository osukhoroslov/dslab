use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::fmt::Debug;
use std::rc::Rc;
use decorum::R64;
use rand::prelude::*;
use rand_pcg::Pcg64;

use crate::actor::*;


// EVENT ENTRY /////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
struct EventEntry<E: Debug> {
    id: u64,
    time: R64,
    src: ActorId,
    dest: ActorId,
    event: E,
}

impl<E: Debug> Eq for EventEntry<E> {}

impl<E: Debug> PartialEq for EventEntry<E> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<E: Debug> Ord for EventEntry<E> {
    fn cmp(&self, other: &Self) -> Ordering {
        other.time.cmp(&self.time)
            .then_with(|| other.id.cmp(&self.id))
    }
}

impl<E: Debug> PartialOrd for EventEntry<E> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// SIMULATION //////////////////////////////////////////////////////////////////////////////////////

pub struct Simulation<E: Debug> {
    clock: R64,
    actors: HashMap<ActorId, Rc<RefCell<dyn Actor<E>>>>,
    events: BinaryHeap<EventEntry<E>>,
    canceled_events: HashSet<u64>,
    undelivered_events: Vec<EventEntry<E>>,
    event_count: u64,
    rand: Pcg64,
}

impl<E: Debug> Simulation<E> {
    pub fn new(seed: u64) -> Self {        
        Self { 
            clock: R64::from_inner(0.0),
            actors: HashMap::new(),
            events: BinaryHeap::new(),
            canceled_events: HashSet::new(),
            undelivered_events: Vec::new(),
            event_count: 0,
            rand: Pcg64::seed_from_u64(seed),
        }
    }

    pub fn time(&self) -> f64 {
        self.clock.into_inner()
    }

    pub fn add_actor(&mut self, id: &str, actor: Rc<RefCell<dyn Actor<E>>>) {
        self.actors.insert(ActorId(id.to_string()), actor);
    }

    pub fn add_event(&mut self, event: E, src: &str, dest: &str, delay: f64) -> u64 {
        let entry = EventEntry {
            id: self.event_count,
            time: self.clock + delay,
            src: ActorId(src.to_string()),
            dest: ActorId(dest.to_string()),
            event
        };
        let id = entry.id;
        self.events.push(entry);
        self.event_count += 1;
        id
    }

    pub fn cancel_event(&mut self, event_id: u64) {
        self.canceled_events.insert(event_id);
    }

    pub fn step(&mut self) -> bool {
        if let Some(e) = self.events.pop() {
            if !self.canceled_events.remove(&e.id) {
                // println!("{} {}->{} {:?}", e.time, e.src, e.dest, e.event);
                self.clock = e.time;
                let actor = self.actors.get(&e.dest);
                let mut ctx = ActorContext{
                    id: e.dest.clone(), 
                    time: self.clock.into_inner(), 
                    rand: &mut self.rand, 
                    next_event_id: self.event_count,
                    events: Vec::new(),
                    canceled_events: Vec::new(),
                };
                match actor {
                    Some(actor) => {
                        if actor.borrow().is_active() {
                            actor.borrow_mut().on(e.event, e.src, e.id, &mut ctx);
                            let canceled = ctx.canceled_events.clone();
                            for ctx_e in ctx.events {
                                self.add_event(ctx_e.event, &e.dest.to(), &ctx_e.dest.to(), ctx_e.delay);
                            };
                            for event_id in canceled {
                                self.cancel_event(event_id);
                            };
                        } else {
                            //println!("Discarded event for inactive actor {}", e.dest);
                        }
                    }
                    _ => {
                        self.undelivered_events.push(e);
                    }
                }
            }
            true
        } else {
            false
        }
    }

    pub fn steps(&mut self, step_count: u32) -> bool {
        for _i in 0..step_count {
            if !self.step() {
                return false
            }
        }
        true
    }

    pub fn step_until_no_events(&mut self) {
        while self.step() {
        }
    }

    pub fn step_for_duration(&mut self, duration: f64) {
        let end_time = self.time() + duration;
        while self.step() && self.time() < end_time {
        }
    }
}
