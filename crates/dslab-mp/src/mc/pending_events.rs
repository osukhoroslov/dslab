use std::collections::BTreeMap;
use std::collections::BTreeSet;

use crate::mc::dependency_resolver::DependencyResolver;
use crate::mc::events::{McEvent, McEventId, SystemTime};

use super::events::DeliveryOptions;

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
        let proc = match &event {
            McEvent::MessageReceived {
                dest,
                options: DeliveryOptions::NoFailures(max_delay),
                ..
            } => {
                let blocked_events =
                    self.resolver
                        .add_message(dest.clone(), self.get_global_time(dest) + *max_delay, id);
                self.available_events.insert(id);
                if let Some(blocked_events) = blocked_events {
                    self.available_events.retain(|e| !blocked_events.contains(e));
                }
                dest
            }
            McEvent::MessageReceived { dest, .. } => {
                self.available_events.insert(id);
                dest
            }
            McEvent::TimerFired {
                proc,
                timer_delay,
                timer,
            } => {
                self.timer_mapping.insert((proc.clone(), timer.clone()), id);
                let (is_available, blocked_events) =
                    self.resolver
                        .add_timer(proc.clone(), self.get_global_time(proc) + *timer_delay, id);
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
                start_time: self.get_global_time(proc),
                event,
            },
        );
        id
    }

    fn get_global_time(&self, proc: &String) -> SystemTime {
        self.global_time.get(proc).map(|x| *x).unwrap_or_default()
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
            McEvent::TimerFired { timer_delay, proc, .. } => {
                assert!(self.get_global_time(proc) <= event_timed.start_time + *timer_delay);
                self.global_time.insert(proc.to_string(), event_timed.start_time + *timer_delay);
            }
            _ => {}
        }
        println!("new times: {:?}", self.global_time);
        event_timed.event
    }
}
