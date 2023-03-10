use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::ops::Bound::{Excluded, Unbounded};

use dslab_core::cast;
use ordered_float::OrderedFloat;

use crate::events::{MessageReceived, TimerFired};
use dslab_core::component::Id;
use dslab_core::event::Event;
use dslab_core::event::EventId;

/// Tracks and enforces dependencies between the TimerFired events on the same node.
/// Any timer with time T should be fired before any timer with time T+x.  
#[derive(Default)]
struct TimerDependencyResolver {
    node_timers: HashMap<Id, BTreeMap<OrderedFloat<f64>, HashSet<EventId>>>,
    pending_messages: HashMap<Id, (BTreeMap<OrderedFloat<f64>, HashSet<EventId>>, HashMap<EventId, OrderedFloat<f64>>)>,
    event_to_node: HashMap<EventId, Id>,
}

#[derive(PartialEq, Eq)]
enum NetworkMode {
    NetworkStable(OrderedFloat<f64>),
    NetworkUnstable,
}

impl Default for NetworkMode {
    fn default() -> Self {
        NetworkMode::NetworkStable(OrderedFloat::<f64>(1.0))
    }
}

impl TimerDependencyResolver {
    pub fn add_timer(&mut self, node: Id, time: f64, event: EventId) -> (bool, Option<HashSet<EventId>>) {
        self.add_event_to_node_mapping(node, event);

        let timers = self.node_timers.entry(node).or_default();
        timers.entry(OrderedFloat(time)).or_default().insert(event);

        let prev_time = timers.range(..OrderedFloat(time)).next_back();
        let mut is_available = prev_time.is_none();

        let messages = self.pending_messages.entry(node).or_default();
        if let Some((timer, _)) = messages.0.iter().next() {
            if *timer < OrderedFloat(time) {
              is_available = false;  
            }
        }

        let next_time = timers.range((Excluded(OrderedFloat(time)), Unbounded)).next();
        let blocked_events = next_time.map(|e| e.1).cloned();

        (is_available, blocked_events)
    }

    pub fn add_message(&mut self, node: Id, time: f64, event: EventId) -> HashSet<EventId> {
        self.add_event_to_node_mapping(node, event);
        let messages = self.pending_messages.entry(node).or_default();
        messages.0.entry(OrderedFloat(time)).or_default().insert(event);
        messages.1.insert(event, OrderedFloat(time));

        let timers = self.node_timers.entry(node).or_default();
        if let Some((timer, first_group)) = timers.iter().next() {
            if *timer > OrderedFloat(time) {
                first_group.clone()
            } else {
                HashSet::default()
            }
        } else {
            HashSet::default()
        }
    }

    fn add_event_to_node_mapping(&mut self, node: Id, event: EventId) {
        assert!(
            self.event_to_node.insert(event, node).is_none(),
            "duplicate EventId not allowed"
        );
    }

    fn get_min_time_message(&self, node: Id) -> Option<&OrderedFloat<f64>> {
        if let Some(messages) = self.pending_messages.get(&node) {
            messages.0.iter().next().map(|x| x.0)
        } else {
            None
        }
    }

    pub fn pop(&mut self, event: EventId) -> Option<HashSet<EventId>> {
        let node = self.event_to_node.remove(&event).unwrap();
        let messages = self.pending_messages.entry(node).or_default();
        if let Some(time) = messages.1.remove(&event) {
            let message_group = messages.0.get_mut(&time).unwrap();
            message_group.remove(&event);
            if message_group.is_empty() {
                messages.0.remove(&time);
            }
            if let Some(new_min_time) = self.get_min_time_message(node) {
                let new_min_time = *new_min_time;
                if time < new_min_time {
                    // some timers might have become available
                    let timers = self.node_timers.get_mut(&node);
                    if let Some(timers) = timers {
                        if let Some((next_timer_time, timer_group)) = timers.range((Unbounded, Excluded(new_min_time))).next() {
                            if *next_timer_time < new_min_time {
                                return Some(timer_group.clone()); 
                            }
                        }
                    }
                }
                None
            } else {
                let timers = self.node_timers.get_mut(&node);
                if let Some(timers) = timers {
                    if let Some((_, timer_group)) = timers.iter().next() {
                        return Some(timer_group.clone()); 
                    }
                }
                None
            }
        } else {
            // check that timer is not blocked by messages
            let timers = self.node_timers.get(&node).unwrap();
            let (timer_time, _) = timers.iter().next().unwrap();
            let timer_time = *timer_time;
            if let Some(min_message_time) = self.get_min_time_message(node) {
                assert!(timer_time <= *min_message_time, "timer is blocked by message");
            }

            // check that timer is really first timer
            let timers = self.node_timers.get_mut(&node).unwrap();
            let (_, events) = timers.iter_mut().next().unwrap();
            assert!(events.remove(&event), "event to pop was not first in queue");
            
            // update timer groups
            if events.is_empty() {
                let timers_mut = self.node_timers.get_mut(&node).unwrap();
                timers_mut.pop_first();
                if let Some((next_timers_time, next_events)) = timers_mut.iter().next() {
                    let next_timers_time = *next_timers_time;
                    let next_events = next_events.clone();
                    if let Some(min_message_time) = self.get_min_time_message(node) {
                        if *min_message_time < next_timers_time {
                            None
                        } else {
                            Some(next_events)
                        }
                    } else {
                        Some(next_events)
                    }
                } else {
                    None
                }
            } else {
                None
            }
        }
    }
}

#[derive(Default)]
pub struct DependencyResolver {
    available_events: HashSet<EventId>,
    timer_resolver: TimerDependencyResolver,
    network_mode: NetworkMode,
}

impl DependencyResolver {
    pub fn new() -> Self {
        DependencyResolver {
            available_events: HashSet::default(),
            timer_resolver: TimerDependencyResolver::default(),
            network_mode: NetworkMode::default(),
        }
    }

    pub fn add_event(&mut self, event: Event) {
        cast!(match event.data {
            MessageReceived { msg: _, src: _, dest: _ } => {
                if let NetworkMode::NetworkStable(rtt) = self.network_mode {
                    let blocked_events = self.timer_resolver.add_message(event.dest, event.time + rtt.0, event.id);
                    self.available_events.insert(event.id);
                    self.available_events.retain(|e| !blocked_events.contains(e));
                }
            }
            TimerFired { proc: _, timer: _ } => {
                let (is_available, blocked_events) = self.timer_resolver.add_timer(event.src, event.time, event.id);
                if is_available {
                    self.available_events.insert(event.id);
                }
                if let Some(blocked) = blocked_events {
                    self.available_events.retain(|e| !blocked.contains(e));
                }
            }
        });
    }

    pub fn available_events(&self) -> &HashSet<EventId> {
        &self.available_events
    }

    pub fn pop_event(&mut self, event_id: EventId) {
        assert!(self.available_events.remove(&event_id));
        if let Some(unblocked_events) = self.timer_resolver.pop(event_id) {
            self.available_events.extend(unblocked_events);
        };
    }
}

#[cfg(test)]
mod tests {
    use crate::message::Message;

    use super::*;
    use rand::prelude::IteratorRandom;
    use rand::prelude::SliceRandom;

    #[test]
    fn test_ordered_float() {
        let a = OrderedFloat(0.0);
        let b = OrderedFloat(0.0);
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
                    data: Box::new(TimerFired {
                        proc: "0".to_owned(),
                        timer: format!("{}", event_time),
                    }),
                };
                resolver.add_event(event);
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
                    data: Box::new(TimerFired {
                        proc: "0".to_owned(),
                        timer: format!("{}", event_time),
                    }),
                };
                resolver.add_event(event);
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
                data: Box::new(TimerFired {
                    proc: "0".to_owned(),
                    timer: "0".to_owned(),
                }),
            };
            resolver.add_event(event);
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
                    data: Box::new(TimerFired {
                        proc: "0".to_owned(),
                        timer: format!("{}", event_time),
                    }),
                };
                resolver.add_event(event);
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

    #[test]
    fn test_timer_dependency_resolver_stable_network() {
        let mut resolver = DependencyResolver::new();
        let mut sequence = Vec::new();
        let times: Vec<u64> = (0..20).into_iter().collect();
        for event_time in times {
            println!("{}", event_time);
            let time = event_time.clamp(0, 11);
            if time == 10 {
                continue;
            }
            let event = Event {
                id: event_time,
                src: 0,
                dest: 0,
                time: time as f64,
                data: Box::new(TimerFired {
                    proc: "0".to_owned(),
                    timer: format!("{}", event_time),
                }),
            };
            resolver.add_event(event);
        }
        let message_times: Vec<u64> = (1..10).step_by(2).into_iter().collect();
        for message_time in message_times {
            println!("{}", message_time);
            let event = Event {
                id: message_time + 100,
                src: 0,
                dest: 0,
                time: message_time as f64,
                data: Box::new(MessageReceived {
                    msg: Message {
                        tip: "a".to_owned(),
                        data: "hello".to_owned(),
                    },
                    src: "0".to_owned(),
                    dest: "0".to_owned(),
                }),
            };
            resolver.add_event(event);
        }

        let count_timers_available = |available: &HashSet<EventId>| {
            available.iter().filter(|x| **x < 100).count()
        };
        let count_messages_available = |available: &HashSet<EventId>| {
            available.len() - count_timers_available(available)
        };


        assert!(count_timers_available(resolver.available_events()) == 1);
        assert!(count_messages_available(resolver.available_events()) == 5);
        while let Some(id) = resolver.available_events().iter().choose(&mut rand::thread_rng()) {
            println!("{:?}", resolver.available_events());
            let id = *id;
            println!("{}", id);
            sequence.push(id);
            resolver.pop_event(id);
            if count_timers_available(resolver.available_events()) > 1 {
                assert!(id % 100 == 9);
                break;
            }
            assert!(count_timers_available(resolver.available_events()) <= 1);
            assert!(count_messages_available(resolver.available_events()) <= 5);
        }
    }

    #[test]
    fn test_timer_dependency_resolver_message_blocks_timer() {
        let mut resolver = DependencyResolver::new();
        let mut sequence = Vec::new();
        for timer in 0..20 {
            let event = Event {
                id: timer,
                src: 0,
                dest: 0,
                time: 10.0 * (1.0 + (timer / 10) as f64),
                data: Box::new(TimerFired {
                    proc: "0".to_owned(),
                    timer: format!("{}", timer),
                }),
            };
            resolver.add_event(event);
        }
        let message = Event {
            id: 100,
            src: 0,
            dest: 0,
            time: 1.0 as f64,
            data: Box::new(MessageReceived {
                msg: Message {
                    tip: "a".to_owned(),
                    data: "hello".to_owned(),
                },
                src: "0".to_owned(),
                dest: "0".to_owned(),
            }),
        };
        resolver.add_event(message);
        

        let count_timers_available = |available: &HashSet<EventId>| {
            available.iter().filter(|x| **x < 100).count()
        };
        let count_messages_available = |available: &HashSet<EventId>| {
            available.len() - count_timers_available(available)
        };

        assert!(count_timers_available(resolver.available_events()) == 0);
        assert!(count_messages_available(resolver.available_events()) == 1);
        resolver.pop_event(100);
        println!("{:?}", resolver.available_events());
        assert!(count_timers_available(resolver.available_events()) == 10);
        assert!(count_messages_available(resolver.available_events()) == 0);

        while let Some(id) = resolver.available_events().iter().choose(&mut rand::thread_rng()) {
            println!("{:?}", resolver.available_events());
            let id = *id;
            println!("{}", id);
            sequence.push(id);
            resolver.pop_event(id);
            assert!(count_timers_available(resolver.available_events()) <= 10);
        }
    }
}
