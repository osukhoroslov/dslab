use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};

use crate::mc::node::McNodeState;
use crate::mc::pending_events::PendingEvents;

#[derive(Debug)]
pub struct McState {
    pub node_states: BTreeMap<String, McNodeState>,
    pub events: PendingEvents,
    pub search_depth: u64,
}

impl McState {
    pub fn new(events: PendingEvents, search_depth: u64) -> Self {
        Self {
            node_states: BTreeMap::new(),
            events,
            search_depth,
        }
    }
}

impl PartialEq for McState {
    fn eq(&self, other: &Self) -> bool {
        self.events == other.events && self.node_states == other.node_states
    }
}

impl Eq for McState {}

impl Hash for McState {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        self.events.hash(hasher);
        self.node_states.hash(hasher);
    }
}
