use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use dslab_core::{Event, EventHandler, Id};

use crate::mc::node::{McNode, McNodeState};
use crate::network::Network;

pub struct McState {
    pub node_states: HashMap<String, McNodeState>,
    pub events: Vec<Event>,
}

impl McState {
    pub fn new(events: Vec<Event>) -> Self {
        Self {
            node_states: HashMap::new(),
            events,
        }
    }
}

pub struct McSystem {
    net: Rc<RefCell<Network>>,
    nodes: HashMap<String, Rc<RefCell<McNode>>>,
    proc_names: Rc<RefCell<Vec<String>>>,
    pub(crate) events: Rc<RefCell<Vec<Event>>>,
    pub(crate) event_count: Rc<RefCell<u64>>,
}

impl McSystem {
    pub fn new(
        net: Rc<RefCell<Network>>,
        nodes: HashMap<String, Rc<RefCell<McNode>>>,
        proc_names: Rc<RefCell<Vec<String>>>,
        events: Rc<RefCell<Vec<Event>>>,
        event_count: Rc<RefCell<u64>>,
    ) -> Self {
        Self {
            net,
            nodes,
            proc_names,
            events,
            event_count,
        }
    }

    fn lookup_proc_name(&self, id: Id) -> String {
        self.proc_names.borrow()[id as usize].clone()
    }

    pub fn apply_event(&mut self, event: Event) {
        self.nodes[&self.lookup_proc_name(event.dest)].borrow_mut().on(event);
    }

    pub fn get_state(&self) -> McState {
        let mut state = McState::new(self.events.borrow().clone());
        for (name, node) in &self.nodes {
            state.node_states.insert(name.clone(), node.borrow().get_state());
        }
        state
    }

    pub fn set_state(&mut self, state: McState) {
        for (name, node_state) in state.node_states {
            self.nodes[&name].borrow_mut().set_state(node_state);
        }
        *self.events.borrow_mut() = state.events;
    }
}
