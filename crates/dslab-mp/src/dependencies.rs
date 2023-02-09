use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::vec::Vec;

use float_ord::FloatOrd;
use serde::Serialize;

use dslab_core::component::Id;
use dslab_core::event::Event;
use dslab_core::event::EventId;

///
/// Timer Dependency Resolver stores queue with timers and create dependencies between timers on one node based on their time (because timer with t_0 happens earlier than t_0 + t)
/// It has several priority queues for each node and groups timers with same time
///
struct TimerDependencyResolver {
    node_timers: HashMap<Id, BTreeMap<FloatOrd<f64>, Vec<EventId>>>,
    event_to_node: HashMap<EventId, Id>,
}

pub struct DependencyResolver {
    available_events: HashSet<EventId>,
    timer_resolver: TimerDependencyResolver,
}

impl TimerDependencyResolver {
    pub fn new() -> Self {
        TimerDependencyResolver {
            node_timers: HashMap::new(),
            event_to_node: HashMap::new(),
        }
    }

    pub fn add(&mut self, node: Id, time: f64, event: EventId) -> (bool, Vec<EventId>) {
        assert!(
            self.event_to_node.insert(event, node).is_none(),
            "duplicate EventId not allowed"
        );
        let timers = self.node_timers.entry(node).or_default();
        let mut new_unavailable = Vec::new();
        let mut is_now_available = false;
        let min_time_after = timers.range(FloatOrd(time)..).next();
        if let Some(next_events) = min_time_after.map(|x| x.1) {
            // next_events might become unavailable
            new_unavailable = next_events.clone();
        }
        let timer_group = timers.entry(FloatOrd(time)).or_default();
        timer_group.push(event);
        let max_time_before = timers.range(..FloatOrd(time)).next_back();
        if max_time_before.is_none() {
            // new event is available
            is_now_available = true;
        }
        (is_now_available, new_unavailable)
    }

    pub fn pop(&mut self, event_id: EventId) -> Vec<EventId> {
        let node = self.event_to_node.remove(&event_id).unwrap();
        let node_timers = self.node_timers.get_mut(&node).unwrap();
        let mut new_available_events = Vec::new();
        let (timer, list) = node_timers.iter_mut().next().unwrap();
        let timer = *timer;
        let idx = list.iter().position(|elem| *elem == event_id);
        assert!(idx.is_some(), "event to pop was not first in queue");
        let idx = idx.unwrap();
        list.remove(idx);
        if list.is_empty() {
            node_timers.remove(&timer).unwrap();
            if let Some(data) = node_timers.iter().next() {
                new_available_events.extend(data.1);
            }
        }
        new_available_events
    }
}

impl DependencyResolver {
    pub fn new() -> Self {
        DependencyResolver {
            available_events: HashSet::default(),
            timer_resolver: TimerDependencyResolver::new(),
        }
    }

    pub fn add_event(&mut self, event: &Event) {
        let dependent_event = event.id;

        let time = event.time;

        let (now_available, new_unavailable) = self.timer_resolver.add(event.src, time, dependent_event.clone());
        if now_available {
            self.available_events.insert(dependent_event);
        }
        // earlier events can now be blocked
        self.available_events
            .retain(|elem| new_unavailable.iter().find(|x| *x == elem).is_none());
    }

    pub fn available_events(&self) -> &HashSet<EventId> {
        &self.available_events
    }

    pub fn pop_event(&mut self, event_id: EventId) {
        self.available_events.remove(&event_id);
        let next_events = self.timer_resolver.pop(event_id);
        for dependency in next_events.iter() {
            self.available_events.insert(dependency.clone());
        }
    }
}

#[derive(Serialize)]
struct SamplePayload {}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::prelude::IteratorRandom;
    use rand::prelude::SliceRandom;

    #[test]
    fn test_float_ord() {
        let a = FloatOrd(0.0);
        let b = FloatOrd(0.0);
        assert!(b <= a);
        assert!(a <= b);
        assert!(a == b);
    }

    #[test]
    fn test_dependency_resolver_simple() {
        let mut resolver = DependencyResolver::new();
        let mut sequence = Vec::new();
        for node_id in 0..3 {
            let mut times: Vec<u64> = (0..3).into_iter().collect();
            times.shuffle(&mut rand::thread_rng());
            for event_time in times {
                let event = Event {
                    id: event_time * 3 + node_id,
                    src: node_id as u32,
                    dest: 0,
                    time: event_time as f64,
                    data: Box::new(SamplePayload {}),
                };
                resolver.add_event(&event);
            }
        }
        while let Some(id) = resolver.available_events().iter().choose(&mut rand::thread_rng()) {
            let id = *id;
            sequence.push(id);
            resolver.pop_event(id);
        }
        println!("{:?}", sequence);
        assert!(sequence.len() == 9);
        let mut timers = vec![0, 0, 0];
        for event_id in sequence {
            let time = event_id / 3;
            let node = event_id % 3;
            assert!(timers[node as usize] == time);
            timers[node as usize] += 1;
        }
    }

    #[test]
    fn test_dependency_resolver_pop() {
        let mut resolver = DependencyResolver::new();
        let mut sequence = Vec::new();
        for node_id in 0..3 {
            let mut times: Vec<u64> = (0..3).into_iter().collect();
            times.shuffle(&mut rand::thread_rng());
            for event_time in times {
                let event = Event {
                    id: event_time * 3 + node_id,
                    src: node_id as u32,
                    dest: 0,
                    time: event_time as f64,
                    data: Box::new(SamplePayload {}),
                };
                resolver.add_event(&event);
            }
        }

        // remove most of elements
        // timer resolver should clear its queues before it
        // can add next events without broken dependencies
        for _ in 0..7 {
            let id = *resolver
                .available_events()
                .iter()
                .choose(&mut rand::thread_rng())
                .unwrap();
            sequence.push(id);
            resolver.pop_event(id);
        }
        for node_id in 0..3 {
            let event = Event {
                id: 9 + node_id,
                src: node_id as u32,
                dest: 0,
                time: 3.0,
                data: Box::new(SamplePayload {}),
            };
            resolver.add_event(&event);
        }
        while let Some(id) = resolver.available_events().iter().choose(&mut rand::thread_rng()) {
            let id = *id;
            sequence.push(id);
            resolver.pop_event(id);
        }
        println!("{:?}", sequence);
        assert!(sequence.len() == 12);
        let mut timers = vec![0, 0, 0];
        for event_id in sequence {
            let time = event_id / 3;
            let node = event_id % 3;
            assert!(timers[node as usize] == time);
            timers[node as usize] += 1;
        }
    }

    #[test]
    fn test_timer_dependency_resolver_same_time() {
        let mut resolver = DependencyResolver::new();
        let mut sequence = Vec::new();
        for node_id in 0..1 {
            let mut times: Vec<u64> = (0..100).into_iter().collect();
            times.shuffle(&mut rand::thread_rng());
            for event_time in times {
                println!("{}", event_time);
                let event = Event {
                    id: event_time,
                    src: node_id as u32,
                    dest: 0,
                    time: (event_time / 5) as f64,
                    data: Box::new(SamplePayload {}),
                };
                resolver.add_event(&event);
            }
        }
        while let Some(id) = resolver.available_events().iter().choose(&mut rand::thread_rng()) {
            println!("{:?}", resolver.available_events());
            let id = *id;
            println!("{}", id);
            sequence.push(id);
            resolver.pop_event(id);
        }
        let mut timers = vec![0];
        for event_id in sequence {
            let time = event_id / 5;
            let node = 0;
            assert!(timers[node as usize] <= time);
            timers[node as usize] = time;
        }
    }
}
