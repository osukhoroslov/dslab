use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap};
use std::rc::Rc;

use crate::mc::events::{McEvent, McEventId};
use crate::mc::network::McNetwork;
use crate::mc::node::McNode;
use crate::mc::pending_events::PendingEvents;
use crate::mc::state::McState;

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
}
