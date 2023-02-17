use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::rc::Rc;

use crate::mc::events::McEvent;
use crate::mc::network::McNetwork;
use crate::mc::node::{McNode, McNodeState};

#[derive(Eq, PartialEq, Clone)]
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

impl Hash for McState {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        let mut hash_events = 0;
        for event in self.events.iter() {
            let mut h = DefaultHasher::default();
            event.hash(&mut h);
            hash_events ^= h.finish();   
        }
        hash_events.hash(hasher);
        let mut hash_states = 0;
        for (_, node_state) in self.node_states.iter() {
            for (proc, proc_state) in node_state.iter() {
                let mut h = DefaultHasher::default();
                proc.hash(&mut h);
                proc_state.hash(&mut h);
                hash_states ^= h.finish();
            }
        }
        hash_states.hash(hasher);
    }
}

pub struct McSystem {
    nodes: HashMap<String, McNode>,
    net: Rc<RefCell<McNetwork>>,
    pub(crate) events: Vec<McEvent>,
}

impl McSystem {
    pub fn new(nodes: HashMap<String, McNode>, net: Rc<RefCell<McNetwork>>, events: Vec<McEvent>) -> Self {
        Self { nodes, net, events }
    }

    pub fn apply_event(&mut self, event: McEvent) {
        let new_events = match event {
            McEvent::MessageReceived { msg, src, dest, .. } => {
                let name = self.net.borrow().get_proc_node(&dest).clone();
                self.nodes.get_mut(&name).unwrap().on_message_received(dest, msg, src)
            }
            McEvent::TimerFired { proc, timer } => {
                let name = self.net.borrow().get_proc_node(&proc).clone();
                self.nodes.get_mut(&name).unwrap().on_timer_fired(proc, timer)
            }
            _ => vec![],
        };

        for new_event in new_events {
            match new_event {
                McEvent::TimerCancelled { timer, proc } => {
                    self.events.retain(|event| match event {
                        McEvent::TimerFired {
                            proc: timer_proc,
                            timer: timer_name,
                        } => *timer_proc != proc || *timer_name != timer,
                        _ => true,
                    });
                }
                _ => self.events.push(new_event),
            }
        }
    }

    pub fn get_state(&self, search_depth: u64) -> McState {
        let mut state = McState::new(self.events.clone(), search_depth);
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
    }
}
