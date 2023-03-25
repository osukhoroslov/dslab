use std::collections::BTreeMap;
use std::collections::BTreeSet;

use crate::mc::dependency::DependencyResolver;
use crate::mc::events::{McEvent, McEventId};

/// Stores pending events and provides a convenient interface for working with them.  
#[derive(Default, Clone, Hash, Eq, PartialEq)]
pub struct PendingEvents {
    events: BTreeMap<McEventId, McEvent>,
    timer_mapping: BTreeMap<(String, String), usize>,
    available_events: BTreeSet<McEventId>,
    resolver: DependencyResolver,
    id_counter: McEventId,
}

impl PendingEvents {
    /// Creates a new empty PendingEvents instance.
    pub fn new() -> Self {
        PendingEvents {
            events: BTreeMap::default(),
            timer_mapping: BTreeMap::default(),
            available_events: BTreeSet::default(),
            resolver: DependencyResolver::default(),
            id_counter: 0,
        }
    }

    /// Stores the passed event and returns id assigned to it.
    pub fn push(&mut self, event: McEvent) -> McEventId {
        let id = self.id_counter;
        self.id_counter += 1;
        self.push_with_fixed_id(event, id)
    }

    /// Stores the passed event under the specified id (should not already exist).
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
                if self.resolver.add_timer(proc.clone(), *timer_delay, id) {
                    self.available_events.insert(id);
                }
            }
            McEvent::TimerCancelled { proc, timer } => {
                let unblocked_events = self
                    .resolver
                    .cancel_timer(self.timer_mapping[&(proc.clone(), timer.clone())]);
                self.available_events.extend(unblocked_events);
                return id;
            }
        };
        self.events.insert(id, event);
        id
    }

    /// Returns event by its id.
    pub fn get(&self, id: McEventId) -> Option<&McEvent> {
        self.events.get(&id)
    }

    /// Returns currently available events, i.e. not blocked by other events (see DependencyResolver).
    pub fn available_events(&self) -> &BTreeSet<McEventId> {
        &self.available_events
    }

    /// Removes available event by its id.
    pub fn pop(&mut self, event_id: McEventId) -> McEvent {
        assert!(self.available_events.remove(&event_id), "event is not available");
        let result = self.events.remove(&event_id).unwrap();
        if let McEvent::TimerFired { .. } = result {
            let unblocked_events = self.resolver.remove_timer(event_id);
            self.available_events.extend(unblocked_events);
        }
        result
    }
}
