use dslab_core::component::Id;
use dslab_core::event::Event;
use dslab_core::event::EventId;
use itertools::Itertools;
use rand::prelude::SliceRandom;
use serde::Serialize;
use std::cell::RefCell;
use std::hash::Hash;

use std::collections::HashMap;
use std::fmt::Debug;
use std::rc::Rc;
use std::vec::Vec;

#[derive(Debug)]
struct DependencyWrapper<T: Copy + PartialEq + Debug> {
    pub inner: T,
    dependencies_before: Vec<Rc<RefCell<DependencyWrapper<T>>>>,
    dependencies_after: Vec<Rc<RefCell<DependencyWrapper<T>>>>,
}

///
/// Timer Dependency Resolver stores queue with timers and create dependencies between timers on one node based on their time (because timer with t_0 happens earlier than t_0 + t)
/// It has several queues for each node and groups timers with same time
/// It is guaranteed that if t is not the earliest timer it is connected with at least one timer before it in queue.
///
struct TimerDependencyResolver {
    node_timers: HashMap<Id, Vec<(f64, Rc<RefCell<DependencyWrapper<EventId>>>)>>,
    event_to_node: HashMap<EventId, Id>,
}

pub struct DependencyResolver {
    available_events: Vec<Rc<RefCell<DependencyWrapper<EventId>>>>,
    timer_resolver: TimerDependencyResolver,
}

impl TimerDependencyResolver {
    pub fn new() -> Self {
        TimerDependencyResolver {
            node_timers: HashMap::new(),
            event_to_node: HashMap::new(),
        }
    }
    pub fn add(&mut self, node: Id, time: f64, event: Rc<RefCell<DependencyWrapper<EventId>>>) {
        assert!(
            self.event_to_node.insert(event.as_ref().borrow().inner, node).is_none(),
            "duplicate EventId not allowed"
        );
        let timers = self.node_timers.entry(node).or_default();
        let mut max_time_before_idx = None;
        let mut min_time_after_idx = None;
        for (idx, timer) in timers.iter().enumerate() {
            if timer.0 < time {
                max_time_before_idx = Some(idx);
            }
            if timer.0 > time && min_time_after_idx.is_none() {
                min_time_after_idx = Some(idx);
            }
        }
        if let Some(min_time_after_idx) = min_time_after_idx {
            for event_after in timers.iter() {
                if event_after.0 == timers[min_time_after_idx].0 {
                    println!(
                        "{} -> {}",
                        event.as_ref().borrow().inner,
                        event_after.1.as_ref().borrow().inner
                    );
                    event.as_ref().borrow_mut().add_child(&event_after.1);
                    event_after.1.as_ref().borrow_mut().add_parent(&event);
                }
            }
        }
        if let Some(idx) = max_time_before_idx {
            let (before, after) = timers.split_at(idx + 1);
            *timers = before
                .into_iter()
                .cloned()
                .chain(vec![(time, event.clone())].iter().cloned())
                .chain(after.into_iter().cloned())
                .collect();
            println!(
                "{} -> {}",
                timers[idx].1.as_ref().borrow().inner,
                event.as_ref().borrow().inner
            );
            timers[idx].1.as_ref().borrow_mut().add_child(&event);
            event.as_ref().borrow_mut().add_parent(&timers[idx].1);
        } else {
            *timers = vec![(time, event)]
                .into_iter()
                .chain(timers.as_slice().into_iter().cloned())
                .collect();
        }
    }

    fn find(&self, node: &Id, event_id: EventId) -> Option<usize> {
        for (idx, elem) in self.node_timers.get(node).unwrap().iter().enumerate() {
            if elem.1.as_ref().borrow().inner == event_id {
                return Some(idx);
            }
        }
        None
    }

    pub fn pop(&mut self, event_id: EventId, childs: &Vec<Rc<RefCell<DependencyWrapper<EventId>>>>) {
        let node = self.event_to_node.remove(&event_id).unwrap();
        let data = self.node_timers.get_mut(&node).unwrap();
        assert!(data.len() > 0, "cannot pop from empty vector");
        let event_pos = self.find(&node, event_id);
        assert!(event_pos.is_some());
        let data = self.node_timers.get_mut(&node).unwrap();
        let event_pos = event_pos.unwrap();
        assert!(data[event_pos].0 == data[0].0);

        let mut other_idx = None;
        for (idx, timer) in data.iter().enumerate() {
            if timer.0 <= data[event_pos].0 && idx != event_pos {
                other_idx = Some(idx);
                break;
            }
        }

        if let Some(idx) = other_idx {
            // need to link over removed elem
            for child in childs {
                data[idx].1.as_ref().borrow_mut().add_child(&child);
                child.as_ref().borrow_mut().add_parent(&data[idx].1);
            }
        }
        data.remove(event_pos);
    }
}

impl DependencyResolver {
    pub fn new() -> Self {
        DependencyResolver {
            available_events: Vec::default(),
            timer_resolver: TimerDependencyResolver::new(),
        }
    }

    pub fn add_event(&mut self, event: &Event) {
        let dependent_event = Rc::new(RefCell::new(DependencyWrapper::<EventId>::new(event.id)));

        let time = event.time;

        self.timer_resolver.add(event.src, time, dependent_event.clone());
        if dependent_event.as_ref().borrow().is_available() {
            self.available_events.push(dependent_event);
        }
        // earlier events can now be blocked
        self.available_events
            .retain(|elem| elem.as_ref().borrow().is_available());
    }

    pub fn available_events(&self) -> Vec<EventId> {
        self.available_events
            .iter()
            .map(|event| event.as_ref().borrow().inner)
            .collect()
    }

    pub fn pop_event(&mut self, event_id: EventId) {
        let event = self
            .available_events
            .iter()
            .enumerate()
            .find(|x| x.1.as_ref().borrow().inner == event_id)
            .unwrap();
        let next_events = event.1.as_ref().borrow_mut().pop_dependencies();
        for dependency in next_events.iter() {
            let idx = dependency
                .as_ref()
                .borrow()
                .dependencies_before
                .iter()
                .enumerate()
                .find(|elem| elem.1.as_ref().borrow().inner == event.1.as_ref().borrow().inner)
                .unwrap()
                .0;
            dependency.borrow_mut().dependencies_before.remove(idx);
        }
        self.available_events.remove(event.0);
        self.timer_resolver.pop(event_id, &next_events);
        for dependency in next_events.iter() {
            if dependency.as_ref().borrow().is_available() {
                self.available_events.push(dependency.clone());
            }
        }
    }
}

impl<T: Copy + PartialEq + Debug + Hash + Eq> DependencyWrapper<T> {
    pub fn new(inner: T) -> Self {
        DependencyWrapper {
            inner,
            dependencies_before: Vec::new(),
            dependencies_after: Vec::new(),
        }
    }

    pub fn add_parent(&mut self, other: &Rc<RefCell<DependencyWrapper<T>>>) {
        self.dependencies_before.push(other.clone());
    }
    pub fn add_child(&mut self, other: &Rc<RefCell<DependencyWrapper<T>>>) {
        self.dependencies_after.push(other.clone());
    }

    pub fn pop_dependencies(&mut self) -> Vec<Rc<RefCell<DependencyWrapper<T>>>> {
        self.dependencies_after
            .drain(..)
            .unique_by(|elem| elem.as_ref().borrow().inner)
            .collect()
    }

    pub fn is_available(&self) -> bool {
        self.dependencies_before.is_empty()
    }
}

#[derive(Serialize)]
struct SamplePayload {}

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
    while let Some(id) = resolver.available_events().choose(&mut rand::thread_rng()) {
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
        let id = *resolver.available_events().choose(&mut rand::thread_rng()).unwrap();
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
    while let Some(id) = resolver.available_events().choose(&mut rand::thread_rng()) {
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
    while let Some(id) = resolver.available_events().choose(&mut rand::thread_rng()) {
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
