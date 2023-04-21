use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::{Hash, Hasher};
use std::rc::Rc;

use crate::mc::events::{McEvent, McEventId};
use crate::mc::network::McNetwork;
use crate::mc::node::{McNode, McNodeState};
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

pub struct McSystem {
    nodes: HashMap<String, McNode>,
    net: Rc<RefCell<McNetwork>>,
    pub(crate) events: PendingEvents,
    search_depth: u64,
}

impl McSystem {
    pub fn new(nodes: HashMap<String, McNode>, net: Rc<RefCell<McNetwork>>, events: PendingEvents) -> Self {
        Self {
            nodes,
            net,
            events,
            search_depth: 0,
        }
    }

    pub fn apply_event(&mut self, event: McEvent) {
        self.search_depth += 1;
        let new_events = match event {
            McEvent::MessageReceived { msg, src, dest, .. } => {
                let name = self.net.borrow().get_proc_node(&dest).clone();
                self.nodes.get_mut(&name).unwrap().on_message_received(dest, msg, src)
            }
            McEvent::TimerFired { proc, timer, .. } => {
                let name = self.net.borrow().get_proc_node(&proc).clone();
                self.nodes.get_mut(&name).unwrap().on_timer_fired(proc, timer)
            }
            _ => vec![],
        };

        for new_event in new_events {
            self.events.push(new_event);
        }
    }

    pub fn get_state(&self) -> McState {
        let mut state = McState::new(self.events.clone(), self.search_depth);
        for (name, node) in &self.nodes {
            state.node_states.insert(name.clone(), node.get_state());
        }
        state
    }

    pub fn set_state(&mut self, state: McState) {
        for (name, node_state) in state.node_states {
            self.nodes.get_mut(&name).unwrap().set_state(node_state);
        }
        self.events = state.events;
        self.search_depth = state.search_depth;
    }

    pub fn available_events(&self) -> BTreeSet<McEventId> {
        self.events.available_events()
    }

    pub fn search_depth(&self) -> u64 {
        self.search_depth
    }
}
