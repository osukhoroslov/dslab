//! Definition of model checking state.

use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};

use crate::mc::events::McEvent;
use crate::mc::node::McNodeState;
use crate::mc::pending_events::PendingEvents;

/// Stores comprehensive information about the state of [`McSystem`](crate::mc::system::McSystem).
/// Used to preserve and restore particular situations in [`McSystem`](crate::mc::system::McSystem).
#[derive(Debug, Clone)]
pub struct McState {
    /// States of nodes in the system.
    pub node_states: BTreeMap<String, McNodeState>,

    /// List of events waiting for delivery.
    pub events: PendingEvents,

    /// Depth of the state in the state graph (i.e. the number of events happened since the initial state).
    pub depth: u64,

    /// Trace in the state graph from initial to current state (i.e. the order of events guiding system to the current state).
    pub trace: Vec<McEvent>,
}

impl McState {
    /// Creates a new state with the specified events in the system and the depth.
    pub fn new(events: PendingEvents, depth: u64, trace: Vec<McEvent>) -> Self {
        Self {
            node_states: BTreeMap::new(),
            events,
            depth,
            trace,
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
