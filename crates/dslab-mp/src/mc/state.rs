//! Definition of model checking state.

use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};

use crate::mc::node::McNodeState;
use crate::mc::pending_events::PendingEvents;

/// Stores comprehensive information about the state of model checking system.
/// Used to preserve and restore particular situations in [`McSystem`](crate::mc::system::McSystem).
#[derive(Debug)]
pub struct McState {
    /// States of nodes in the system.
    pub node_states: BTreeMap<String, McNodeState>,

    /// List of events waiting for delivery.
    pub events: PendingEvents,

    /// Depth in the search graph of model checking algorithm.
    pub search_depth: u64,
}

impl McState {
    /// Creates a new model checking state with the specified events
    /// in the system and the search algorithm depth.
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
