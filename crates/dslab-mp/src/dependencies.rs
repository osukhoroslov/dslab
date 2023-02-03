use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::rc::Rc;
use std::vec::Vec;

use float_ord::FloatOrd;
use itertools::Itertools;
use serde::Serialize;

use dslab_core::component::Id;
use dslab_core::event::Event;
use dslab_core::event::EventId;

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
    node_timers: HashMap<Id, BTreeMap<FloatOrd<f64>, Vec<EventId>>>,
    event_to_dependency: HashMap<EventId, Rc<RefCell<DependencyWrapper<EventId>>>>,
    dependency_stubs: HashMap<Id, HashMap<FloatOrd<f64>, Rc<RefCell<DependencyWrapper<EventId>>>>>,
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
            event_to_dependency: HashMap::new(),
            dependency_stubs: HashMap::new(),
            event_to_node: HashMap::new(),
        }
    }

    pub fn add(&mut self, node: Id, time: f64, event: Rc<RefCell<DependencyWrapper<EventId>>>) {
        assert!(
            self.event_to_node.insert(event.as_ref().borrow().inner, node).is_none(),
            "duplicate EventId not allowed"
        );
        self.event_to_dependency
            .insert(event.as_ref().borrow().inner, event.clone());
        let timers = self.node_timers.entry(node).or_default();
        self.dependency_stubs.entry(node).or_default();
        let min_time_after = timers.range(FloatOrd(time)..).next();
        let next_events = min_time_after.and_then(|x| Some(x.1.clone())).unwrap_or_default();
        let timer_group = timers.entry(FloatOrd(time)).or_insert_with(|| {
            self.dependency_stubs.get_mut(&node).unwrap().insert(
                FloatOrd(time),
                Rc::new(RefCell::new(DependencyWrapper::<EventId>::new(u64::MAX))),
            );

            for next_event in next_events {
                let fake_dependency = &self.dependency_stubs[&node][&FloatOrd(time)];
                let next_event = &self.event_to_dependency[&next_event];
                println!(
                    "{} -> {}",
                    fake_dependency.as_ref().borrow().inner,
                    next_event.as_ref().borrow().inner
                );

                fake_dependency.as_ref().borrow_mut().add_child(&next_event);
                next_event.as_ref().borrow_mut().add_parent(&fake_dependency);
            }
            Vec::default()
        });
        timer_group.push(event.as_ref().borrow().inner);
        let max_time_before = timers.range(..FloatOrd(time)).next_back();
        if let Some((max_time_before, _)) = max_time_before {
            let dependency_before = &self.dependency_stubs[&node][max_time_before];
            println!(
                "{} -> {}",
                dependency_before.as_ref().borrow().inner,
                event.as_ref().borrow().inner
            );
            dependency_before.as_ref().borrow_mut().add_child(&event);
            event.as_ref().borrow_mut().add_parent(&dependency_before);
        }
    }

    pub fn pop(&mut self, event_id: EventId) -> Vec<Rc<RefCell<DependencyWrapper<u64>>>> {
        let node = self.event_to_node.remove(&event_id).unwrap();
        let node_timers = self.node_timers.get_mut(&node).unwrap();
        let mut new_available_events = Vec::new();
        while !node_timers.is_empty() {
            let (timer, list) = node_timers.into_iter().next().unwrap();
            let timer = timer.clone();
            if list.is_empty() {
                node_timers.remove(&timer).unwrap();
                let fake_dependency = &self.dependency_stubs[&node][&timer];
                new_available_events.extend(fake_dependency.as_ref().borrow_mut().pop_dependencies());
                self.dependency_stubs.get_mut(&node).unwrap().remove(&timer);
            } else {
                let idx = list.iter().position(|elem| *elem == event_id);
                assert!(idx.is_some(), "event to pop was not first in queue");
                let idx = idx.unwrap();
                list.remove(idx);
                let dependency = self.event_to_dependency.remove(&event_id).unwrap();
                new_available_events.extend(dependency.as_ref().borrow_mut().pop_dependencies());
                if list.is_empty() {
                    node_timers.remove(&timer).unwrap();
                    let fake_dependency = &self.dependency_stubs[&node][&timer];
                    let tmp_available_events = fake_dependency.as_ref().borrow_mut().pop_dependencies();
                    for event in &tmp_available_events {
                        let idx = event
                            .as_ref()
                            .borrow()
                            .dependencies_before
                            .iter()
                            .enumerate()
                            .find(|elem| elem.1.as_ref().borrow().inner == fake_dependency.as_ref().borrow().inner)
                            .unwrap()
                            .0;
                        event.borrow_mut().dependencies_before.remove(idx);
                    }
                    new_available_events.extend(tmp_available_events.into_iter());
                    self.dependency_stubs.get_mut(&node).unwrap().remove(&timer);
                }
                break;
            }
        }
        new_available_events
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
        self.available_events.remove(event.0);
        let next_events = self.timer_resolver.pop(event_id);
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

#[cfg(test)]
mod tests {
    use super::*;
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
}
