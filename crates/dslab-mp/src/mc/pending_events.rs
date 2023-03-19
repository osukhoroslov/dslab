use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::ops::Bound::{Excluded, Unbounded};

use crate::mc::events::{McEvent, McEventId, SystemTime};

use super::events::DeliveryOptions;

type BidirectionalMapping = (
    BTreeMap<SystemTime, BTreeSet<McEventId>>,
    BTreeMap<McEventId, SystemTime>,
);

/// Tracks and enforces dependencies between the TimerFired events on the same node.
/// Any timer with time T should be fired before any timer with time T+x.  
/// Also keeps track of messages in stable network that can block long timers.
#[derive(Default, Clone, Hash, Eq, PartialEq)]
struct DependencyResolver {
    proc_timers: BTreeMap<String, BidirectionalMapping>,
    pending_messages: BTreeMap<String, BidirectionalMapping>,
    event_to_proc: BTreeMap<McEventId, String>,
}

impl DependencyResolver {
    fn get_all_available_events(&self, proc: &String) -> BTreeSet<McEventId> {
        let messages_iter = self.pending_messages[proc].1.keys().map(|x| *x);
        if let Some((time, timers)) = self.get_min_time_timers(proc) {
            if !self.get_min_time_message(proc).map_or(false, |t| t < time) {
                return BTreeSet::from_iter(messages_iter.chain(timers.clone().into_iter()));
            }
        }
        BTreeSet::from_iter(messages_iter)
    }

    pub fn add_timer(&mut self, proc: String, time: SystemTime, event: McEventId) -> (bool, Option<BTreeSet<McEventId>>) {
        self.add_event_to_node_mapping(proc.clone(), event);

        Self::create_event_mappings(&mut self.proc_timers, &proc, event, time);

        let timer_group = &self.proc_timers[&proc].0;
        let prev_time = timer_group.range(..time).next_back();
        let mut is_available = prev_time.is_none();

        if let Some(timer) = self.get_min_time_message(&proc) {
            if *timer < time {
                is_available = false;
            }
        }

        let next_time = timer_group.range((Excluded(time), Unbounded)).next();
        let blocked_events = next_time.map(|e| e.1).cloned();

        (is_available, blocked_events)
    }

    pub fn add_message(&mut self, proc: String, time: SystemTime, event: McEventId) -> BTreeSet<McEventId> {
        self.add_event_to_node_mapping(proc.clone(), event);
        Self::create_event_mappings(&mut self.pending_messages, &proc, event, time);

        let timers = self.proc_timers.entry(proc).or_default();
        if let Some((timer, first_group)) = timers.0.iter().next() {
            if *timer > SystemTime::from(time) {
                return first_group.clone();
            }
        }
        BTreeSet::default()
    }

    pub fn cancel_timer(&mut self, proc: String, event: McEventId) -> BTreeSet<McEventId> {
        let timers = self.proc_timers.entry(proc.clone()).or_default();
        let time = timers.1[&event];
        timers.0.get_mut(&time).unwrap().remove(&event);
        self.event_to_proc.remove(&event);
        self.get_all_available_events(&proc)
    }

    fn add_event_to_node_mapping(&mut self, proc: String, event: McEventId) {
        assert!(
            self.event_to_proc.insert(event, proc).is_none(),
            "duplicate McEventId not allowed"
        );
    }

    fn create_event_mappings(
        proc_mapping: &mut BTreeMap<String, BidirectionalMapping>,
        proc: &String,
        id: McEventId,
        time: SystemTime,
    ) {
        let mapping = proc_mapping.entry(proc.clone()).or_default();
        mapping.0.entry(SystemTime::from(time)).or_default().insert(id);
        mapping.1.insert(id, time);
    }

    fn get_min_time_message(&self, proc: &String) -> Option<&SystemTime> {
        if let Some(messages) = self.pending_messages.get(proc) {
            messages.0.iter().next().map(|x| x.0)
        } else {
            None
        }
    }

    fn get_min_time_timers(&self, proc: &String) -> Option<(&SystemTime, &BTreeSet<McEventId>)> {
        if let Some(timers) = self.proc_timers.get(proc) {
            if let Some(timer_group) = timers.0.iter().next() {
                Some(timer_group)
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn pop(&mut self, event: McEventId) -> BTreeSet<McEventId> {
        let proc = self.event_to_proc.remove(&event).unwrap();
        let messages = self.pending_messages.entry(proc.clone()).or_default();
        if let Some(time) = messages.1.remove(&event) {
            let message_group = messages.0.get_mut(&time).unwrap();
            message_group.remove(&event);
            if message_group.is_empty() {
                messages.0.remove(&time);
            }
            self.get_all_available_events(&proc)
        } else {
            // check that timer is not blocked by messages
            let timers = self.proc_timers.get(&proc).unwrap();
            let (timer_time, _) = timers.0.iter().next().unwrap();
            let timer_time = *timer_time;
            if let Some(min_message_time) = self.get_min_time_message(&proc) {
                assert!(timer_time <= *min_message_time, "timer is blocked by message");
            }

            // check that timer is really first timer
            let timers = self.proc_timers.get_mut(&proc).unwrap();
            let (min_time, events) = timers.0.iter_mut().next().unwrap();
            let min_time = *min_time;
            assert!(events.remove(&event), "event to pop was not first in queue");
            if events.is_empty() {
                timers.0.remove(&min_time);
            }

            self.get_all_available_events(&proc)
        }
    }
}

#[derive(PartialEq, Eq, Clone)]
#[allow(dead_code)]
enum NetworkMode {
    NetworkStable(SystemTime),
    NetworkUnstable,
}

impl Default for NetworkMode {
    fn default() -> Self {
        NetworkMode::NetworkStable(SystemTime::from(1.0))
    }
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct McEventTimed {
    event: McEvent,
    start_time: SystemTime,
}

#[derive(Default, Clone, Hash, Eq, PartialEq)]
pub struct PendingEvents {
    events: BTreeMap<usize, McEventTimed>,
    timer_mapping: BTreeMap<(String, String), usize>,
    available_events: BTreeSet<McEventId>,
    resolver: DependencyResolver,
    id_counter: usize,
    global_time: BTreeMap<String, SystemTime>,
}

impl PendingEvents {
    pub fn new() -> Self {
        PendingEvents {
            events: BTreeMap::default(),
            timer_mapping: BTreeMap::default(),
            available_events: BTreeSet::default(),
            resolver: DependencyResolver::default(),
            id_counter: 0,
            global_time: BTreeMap::new(),
        }
    }

    pub fn push(&mut self, event: McEvent) -> usize {
        let id = self.id_counter;
        self.id_counter += 1;
        let default = SystemTime::from(0.0);
        let proc = match &event {
            McEvent::MessageReceived {
                dest,
                options: DeliveryOptions::NoFailures(rtt),
                ..
            } => {
                let blocked_events =
                    self.resolver
                        .add_message(dest.clone(), *self.global_time.get(dest).unwrap_or(&default) + *rtt, id);
                self.available_events.retain(|e| !blocked_events.contains(e));
                self.available_events.insert(id);
                dest
            }
            McEvent::MessageReceived { dest, .. } => {
                self.available_events.insert(id);
                dest
            }
            McEvent::TimerFired { proc, duration, timer } => {
                self.timer_mapping.insert((proc.clone(), timer.clone()), id);
                let (is_available, blocked_events) = self.resolver.add_timer(
                    proc.clone(),
                    *self.global_time.get(proc).unwrap_or(&default) + *duration,
                    id,
                );
                if is_available {
                    self.available_events.insert(id);
                }
                if let Some(blocked) = blocked_events {
                    self.available_events.retain(|e| !blocked.contains(e));
                }
                proc
            }
            McEvent::TimerCancelled { proc, timer } => {
                self.resolver
                    .cancel_timer(proc.clone(), self.timer_mapping[&(proc.clone(), timer.clone())]);
                proc
            }
        };
        self.events.insert(
            id,
            McEventTimed {
                start_time: *self.global_time.get(proc).unwrap_or(&default),
                event,
            },
        );
        id
    }

    pub fn get(&self, id: McEventId) -> Option<&McEvent> {
        self.events.get(&id).map(|e| &e.event)
    }

    pub fn get_mut(&mut self, id: McEventId) -> Option<&mut McEvent> {
        self.events.get_mut(&id).map(|e| &mut e.event)
    }

    pub fn available_events(&self) -> &BTreeSet<McEventId> {
        &self.available_events
    }

    pub fn pop(&mut self, event_id: McEventId) -> McEvent {
        assert!(self.available_events.remove(&event_id));
        let unblocked_events = self.resolver.pop(event_id);
        self.available_events.extend(unblocked_events);
        let event_timed = self.events.remove(&event_id).unwrap();
        match &event_timed.event {
            McEvent::TimerFired { duration, proc, .. } => {
                let default = SystemTime::from(0.0);
                assert!(*self.global_time.get(proc).unwrap_or(&default) <= event_timed.start_time + *duration);
                let entry = self.global_time.entry(proc.clone()).or_default();
                *entry = event_timed.start_time + *duration;
            }
            _ => {}
        }
        println!("new times: {:?}", self.global_time);
        event_timed.event
    }
}

#[cfg(test)]
mod tests {
    use crate::mc::events::{DeliveryOptions, McEvent};
    use crate::message::Message;

    use super::*;
    use rand::prelude::IteratorRandom;
    use rand::prelude::SliceRandom;

    #[test]
    fn test_system_time() {
        let a = SystemTime::from(0.0);
        let b = SystemTime::from(0.0);
        assert!(b <= a);
        assert!(a <= b);
        assert!(a == b);
    }

    #[test]
    fn test_dependency_resolver_simple() {
        let mut resolver = PendingEvents::new();
        let mut sequence = Vec::new();
        let mut rev_id = vec![0; 9];
        for node_id in 0..3 {
            let mut times: Vec<u64> = (0..3).into_iter().collect();
            times.shuffle(&mut rand::thread_rng());
            for event_time in times {
                let event = McEvent::TimerFired {
                    proc: node_id.to_string(),
                    timer: format!("{}", event_time),
                    duration: SystemTime::from(event_time as f64),
                };
                rev_id[resolver.push(event)] = event_time * 3 + node_id;
            }
        }
        println!("{:?}", rev_id);
        while let Some(id) = resolver.available_events().iter().choose(&mut rand::thread_rng()) {
            let id = *id;
            sequence.push(rev_id[id]);
            resolver.pop(id);
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
        let mut resolver = PendingEvents::new();
        let mut sequence = Vec::new();
        let mut rev_id = vec![0; 12];

        for node_id in 0..3 {
            let mut times: Vec<u64> = (0..3).into_iter().collect();
            times.shuffle(&mut rand::thread_rng());
            for event_time in times {
                let event = McEvent::TimerFired {
                    proc: node_id.to_string(),
                    timer: format!("{}", event_time),
                    duration: SystemTime::from(1.0 + event_time as f64),
                };
                rev_id[resolver.push(event)] = event_time * 3 + node_id;
            }
        }

        // remove most of elements
        // timer resolver should clear its queues before it
        // can add next events without broken dependencies
        // every process moved its global timer at least once
        for _ in 0..7 {
            let id = *resolver
                .available_events()
                .iter()
                .choose(&mut rand::thread_rng())
                .unwrap();
            sequence.push(rev_id[id]);
            resolver.pop(id);
        }

        // this events would be last
        for node_id in 0..3 {
            let event = McEvent::TimerFired {
                proc: node_id.to_string(),
                timer: format!("{}", node_id),
                duration: SystemTime::from(2.1),
            };
            rev_id[resolver.push(event)] = 9 + node_id;
        }
        while let Some(id) = resolver.available_events().iter().choose(&mut rand::thread_rng()) {
            let id = *id;
            sequence.push(rev_id[id]);
            resolver.pop(id);
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
        let mut resolver = PendingEvents::new();
        let mut sequence = Vec::new();
        let mut rev_id = vec![0; 100];

        for node_id in 0..1 {
            let mut times: Vec<u64> = (0..100).into_iter().collect();
            times.shuffle(&mut rand::thread_rng());
            for event_time in times {
                let event = McEvent::TimerFired {
                    proc: node_id.to_string(),
                    timer: format!("{}", event_time),
                    duration: SystemTime::from((event_time / 5) as f64),
                };
                rev_id[resolver.push(event)] = event_time;
            }
        }
        while let Some(id) = resolver.available_events().iter().choose(&mut rand::thread_rng()) {
            sequence.push(rev_id[*id]);
            resolver.pop(*id);
        }
        println!("{:?}", sequence);
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
        let mut resolver = PendingEvents::new();
        let mut sequence = Vec::new();
        let times: Vec<u64> = (0..20).into_iter().collect();
        let mut rev_id = vec![0; 25];
        for event_time in times {
            let time = event_time.clamp(0, 11);
            if time == 10 {
                continue;
            }
            let event = McEvent::TimerFired {
                proc: "0".to_owned(),
                timer: format!("{}", event_time),
                duration: SystemTime::from(time as f64),
            };
            rev_id[resolver.push(event)] = event_time;
        }
        let message_times: Vec<u64> = (1..10).step_by(2).into_iter().collect();
        for message_time in message_times {
            let event = McEvent::MessageReceived {
                msg: Message {
                    tip: "a".to_owned(),
                    data: "hello".to_owned(),
                },
                src: "0".to_owned(),
                dest: "0".to_owned(),
                options: DeliveryOptions::NoFailures(SystemTime::from(message_time as f64)),
            };
            rev_id[resolver.push(event)] = 20 + message_time / 2;
        }

        println!("{:?}", resolver.available_events());

        let count_timers_available =
            |available: &BTreeSet<McEventId>| available.iter().filter(|x| rev_id[**x] < 20).count();
        let count_messages_available =
            |available: &BTreeSet<McEventId>| available.len() - count_timers_available(available);

        assert!(count_timers_available(resolver.available_events()) == 1);
        assert!(count_messages_available(resolver.available_events()) == 5);
        while let Some(id) = resolver.available_events().iter().choose(&mut rand::thread_rng()) {
            let id = *id;
            println!("{}", rev_id[id]);
            sequence.push(id);
            resolver.pop(id);
            if count_timers_available(resolver.available_events()) > 1 {
                assert!(rev_id[id] == 9 || rev_id[id] == 24);
                break;
            }
            assert!(count_timers_available(resolver.available_events()) <= 1);
            assert!(count_messages_available(resolver.available_events()) <= 5);
        }
    }

    #[test]
    fn test_timer_dependency_resolver_message_blocks_timer() {
        let mut resolver = PendingEvents::new();
        let mut sequence = Vec::new();
        let mut rev_id = vec![0; 25];

        for timer in 0..20 {
            let event = McEvent::TimerFired {
                proc: "0".to_owned(),
                timer: format!("{}", timer),
                duration: SystemTime::from(10.0 * (1.0 + (timer / 10) as f64)),
            };
            rev_id[resolver.push(event)] = timer;
        }
        let message = McEvent::MessageReceived {
            msg: Message {
                tip: "a".to_owned(),
                data: "hello".to_owned(),
            },
            src: "0".to_owned(),
            dest: "0".to_owned(),
            options: DeliveryOptions::NoFailures(SystemTime::from(1.0)),
        };
        let message_id = resolver.push(message);
        rev_id[message_id] = 100;

        let count_timers_available =
            |available: &BTreeSet<McEventId>| available.iter().filter(|x| rev_id[**x] < 20).count();
        let count_messages_available =
            |available: &BTreeSet<McEventId>| available.len() - count_timers_available(available);

        assert!(count_timers_available(resolver.available_events()) == 0);
        assert!(count_messages_available(resolver.available_events()) == 1);
        resolver.pop(message_id);
        println!("{:?}", resolver.available_events());
        assert!(count_timers_available(resolver.available_events()) == 10);
        assert!(count_messages_available(resolver.available_events()) == 0);

        while let Some(id) = resolver.available_events().iter().choose(&mut rand::thread_rng()) {
            println!("{:?}", resolver.available_events());
            let id = *id;
            println!("{}", id);
            sequence.push(id);
            resolver.pop(id);
            assert!(count_timers_available(resolver.available_events()) <= 10);
        }
    }
}
