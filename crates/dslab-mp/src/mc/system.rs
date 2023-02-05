use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use crate::mc::events::McEvent;
use crate::mc::network::McNetwork;
use crate::mc::node::{McNode, McNodeState};

pub struct McState {
    pub node_states: HashMap<String, McNodeState>,
    pub events: Vec<McEvent>,
    pub search_depth: u64,
}

impl McState {
    pub fn new(events: Vec<McEvent>, search_depth: u64) -> Self {
        Self {
            node_states: HashMap::new(),
            events,
            search_depth,
        }
    }
}

pub struct McSystem {
    nodes: HashMap<String, McNode>,
    net: Rc<RefCell<McNetwork>>,
    pub(crate) events: Rc<RefCell<Vec<McEvent>>>,
}

impl McSystem {
    pub fn new(nodes: HashMap<String, McNode>, net: Rc<RefCell<McNetwork>>, events: Rc<RefCell<Vec<McEvent>>>) -> Self {
        Self { nodes, net, events }
    }

    pub fn apply_event(&mut self, event: McEvent) {
        match event {
            McEvent::MessageReceived { msg, src, dest } => {
                self.nodes
                    .get_mut(self.net.borrow().get_proc_node(&src))
                    .unwrap()
                    .on_message_received(dest, msg, src);
            }
            McEvent::TimerFired { proc, timer } => {
                self.nodes
                    .get_mut(self.net.borrow().get_proc_node(&proc))
                    .unwrap()
                    .on_timer_fired(proc, timer);
            }
        }
    }

    pub fn get_state(&self, search_depth: u64) -> McState {
        let mut state = McState::new(self.events.borrow().clone(), search_depth);
        for (name, node) in &self.nodes {
            state.node_states.insert(name.clone(), node.get_state());
        }
        state
    }

    pub fn set_state(&mut self, state: McState) {
        for (name, node_state) in state.node_states {
            self.nodes.get_mut(&name).unwrap().set_state(node_state);
        }
        *self.events.borrow_mut() = state.events;
    }
}
