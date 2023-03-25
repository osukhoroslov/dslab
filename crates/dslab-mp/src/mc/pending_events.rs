use std::collections::BTreeMap;
use std::collections::BTreeSet;

use crate::mc::dependency_resolver::DependencyResolver;
use crate::mc::events::{McEvent, McEventId, McTime};

#[derive(Default, Clone, Hash, Eq, PartialEq)]
pub struct PendingEvents {
    events: BTreeMap<McEventId, McEvent>,
    timer_mapping: BTreeMap<(String, String), usize>,
    available_events: BTreeSet<McEventId>,
    resolver: DependencyResolver,
    id_counter: McEventId,
    global_time: BTreeMap<String, McTime>,
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

    pub fn push(&mut self, event: McEvent) -> McEventId {
        let id = self.id_counter;
        self.id_counter += 1;
        self.push_with_fixed_id(event, id)
    }

    pub(crate) fn push_with_fixed_id(&mut self, event: McEvent, id: McEventId) -> McEventId {
        assert!(!self.events.contains_key(&id), "event with such id already exists");
        match &event {
            McEvent::MessageReceived { .. } => {
                self.available_events.insert(id);
            }
            McEvent::TimerFired {
                proc,
                timer_delay,
                timer,
            } => {
                self.timer_mapping.insert((proc.clone(), timer.clone()), id);
                if self
                    .resolver
                    .add_timer(proc.clone(), self.get_global_time(proc) + *timer_delay, id)
                {
                    self.available_events.insert(id);
                }
            }
            McEvent::TimerCancelled { proc, timer } => {
                let unblocked_events = self
                    .resolver
                    .cancel_timer(proc.clone(), self.timer_mapping[&(proc.clone(), timer.clone())]);
                self.available_events.extend(unblocked_events);
                return id;
            }
        };
        self.events.insert(id, event);
        id
    }

    fn get_global_time(&self, proc: &String) -> McTime {
        self.global_time.get(proc).copied().unwrap_or_default()
    }

    pub fn get(&self, id: McEventId) -> Option<&McEvent> {
        self.events.get(&id)
    }

    pub fn get_mut(&mut self, id: McEventId) -> Option<&mut McEvent> {
        self.events.get_mut(&id)
    }

    pub fn available_events(&self) -> &BTreeSet<McEventId> {
        &self.available_events
    }

    pub fn pop(&mut self, event_id: McEventId) -> McEvent {
        println!("{}", event_id);
        assert!(self.available_events.remove(&event_id));
        let result = self.events.remove(&event_id);
        let result = result.unwrap();
        if let McEvent::TimerFired { .. } = result {
            let unblocked_events = self.resolver.pop(event_id);
            self.available_events.extend(unblocked_events);
        }
        result
    }
}
