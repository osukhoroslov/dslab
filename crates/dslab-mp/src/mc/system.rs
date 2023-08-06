use std::cell::RefCell;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeSet, HashMap};
use std::hash::{Hash, Hasher};
use std::rc::Rc;

use ordered_float::OrderedFloat;

use crate::logger::LogEntry;
use crate::mc::events::{McEvent, McEventId};
use crate::mc::network::McNetwork;
use crate::mc::node::McNode;
use crate::mc::pending_events::PendingEvents;
use crate::mc::state::McState;
use crate::mc::trace_handler::TraceHandler;
use crate::message::Message;

pub type McTime = OrderedFloat<f64>;

pub struct McSystem {
    nodes: HashMap<String, McNode>,
    net: Rc<RefCell<McNetwork>>,
    pub(crate) events: PendingEvents,
    depth: u64,
    trace_handler: Rc<RefCell<TraceHandler>>,
}

impl McSystem {
    pub fn new(
        nodes: HashMap<String, McNode>,
        net: Rc<RefCell<McNetwork>>,
        events: PendingEvents,
        trace_handler: Rc<RefCell<TraceHandler>>,
    ) -> Self {
        Self {
            nodes,
            net,
            events,
            depth: 0,
            trace_handler,
        }
    }

    pub fn apply_event(&mut self, event: McEvent) {
        self.depth += 1;
        self.trace_handler.borrow_mut().push(event.to_log_entry());
        let event_time = Self::get_approximate_event_time(self.depth);
        let state_hash = self.get_state_hash();
        let new_events = match event {
            McEvent::MessageReceived { msg, src, dst, .. } => {
                let name = self.net.borrow().get_proc_node(&dst).clone();
                self.nodes
                    .get_mut(&name)
                    .unwrap()
                    .on_message_received(dst, msg, src, event_time, state_hash)
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

    pub fn send_local_message<S>(&mut self, node: S, proc: S, msg: Message)
    where
        S: Into<String>,
    {
        let node = node.into();
        let proc = proc.into();
        let event_time = Self::get_approximate_event_time(self.depth);
        let state_hash = self.get_state_hash();

        self.trace_handler.borrow_mut().push(LogEntry::McLocalMessageReceived {
            msg: msg.clone(),
            proc: proc.clone(),
        });
        let new_events = self
            .nodes
            .get_mut(&node)
            .unwrap()
            .on_local_message_received(proc, msg, event_time, state_hash);
        for new_event in new_events {
            self.events.push(new_event);
        }
    }

    pub fn get_state(&self) -> McState {
        let mut state = McState::new(self.events.clone(), self.depth, self.trace_handler.borrow().trace());
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
        self.trace_handler.borrow_mut().set_trace(state.trace);
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
