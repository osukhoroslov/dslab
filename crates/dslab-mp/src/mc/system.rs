use std::cell::RefCell;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeSet, HashMap};
use std::hash::{Hash, Hasher};
use std::rc::Rc;

use crate::mc::events::{McEvent, McEventId};
use crate::mc::network::McNetwork;
use crate::mc::node::McNode;
use crate::mc::pending_events::PendingEvents;
use crate::mc::state::McState;
use crate::message::Message;

pub struct McSystem {
    nodes: HashMap<String, McNode>,
    net: Rc<RefCell<McNetwork>>,
    pub(crate) events: PendingEvents,
    depth: u64,
}

impl McSystem {
    pub fn new(nodes: HashMap<String, McNode>, net: Rc<RefCell<McNetwork>>, events: PendingEvents) -> Self {
        Self {
            nodes,
            net,
            events,
            depth: 0,
        }
    }

    pub fn apply_event(&mut self, event: McEvent) {
        self.depth += 1;
        let event_time = Self::get_approximate_event_time(self.depth);
        let state_hash = self.get_state_hash();
        let new_events = match event {
            McEvent::MessageReceived { msg, src, dest, .. } => {
                let name = self.net.borrow().get_proc_node(&dest).clone();
                self.nodes
                    .get_mut(&name)
                    .unwrap()
                    .on_message_received(dest, msg, src, event_time, state_hash)
            }
            McEvent::TimerFired { proc, timer, .. } => {
                let name = self.net.borrow().get_proc_node(&proc).clone();
                self.nodes
                    .get_mut(&name)
                    .unwrap()
                    .on_timer_fired(proc, timer, event_time, state_hash)
            }
            _ => vec![],
        };

        for new_event in new_events {
            self.events.push(new_event);
        }
    }

    pub fn send_local_message(&mut self, node: String, proc: String, msg: Message) {
        let new_events = self.nodes.get_mut(&node).unwrap().on_local_message_received(proc, msg);
        for new_event in new_events {
            self.events.push(new_event);
        }
    }

    pub fn get_state(&self) -> McState {
        let mut state = McState::new(self.events.clone(), self.depth);
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
        self.depth = state.depth;
    }

    pub fn available_events(&self) -> BTreeSet<McEventId> {
        self.events.available_events()
    }

    pub fn depth(&self) -> u64 {
        self.depth
    }

    fn get_approximate_event_time(depth: u64) -> f64 {
        // every step of system execution in model checking advances the time by 0.1s
        // this makes the time value look more natural and closer to the time in simulation
        depth as f64 / 10.0
    }

    fn get_state_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::default();
        self.get_state().hash(&mut hasher);
        hasher.finish()
    }
}
