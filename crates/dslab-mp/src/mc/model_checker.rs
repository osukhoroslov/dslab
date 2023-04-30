//! Model checker configuration and launching.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use colored::*;

use crate::events::{MessageReceived, TimerFired};
use crate::mc::events::{DeliveryOptions, McEvent, McTime};
use crate::mc::network::McNetwork;
use crate::mc::node::McNode;
use crate::mc::pending_events::PendingEvents;
use crate::mc::strategy::Strategy;
use crate::mc::system::McSystem;
use crate::system::System;
use crate::util::t;

use super::strategy::McResult;
use super::system::McState;

/// Main class of (and entrypoint to) the model checking testing technique.
pub struct ModelChecker<'a> {
    system: McSystem,
    strategy: Box<dyn Strategy + 'a>,
}

impl<'a> ModelChecker<'a> {
    /// Creates a new model checker with the specified strategy
    /// and initial state equal to the current state of the system.
    pub fn new(sys: &System, strategy: Box<dyn Strategy + 'a>) -> Self {
        let sim = sys.sim();

        let insta_network = sys.network().max_delay() == 0.0;
        let mut events = PendingEvents::new(insta_network);
        for event in sim.dump_events() {
            if let Some(value) = event.data.downcast_ref::<MessageReceived>() {
                events.push(McEvent::MessageReceived {
                    msg: value.msg.clone(),
                    src: value.src.clone(),
                    dest: value.dest.clone(),
                    options: DeliveryOptions::NoFailures(McTime::from(sys.network().max_delay())),
                });
            } else if let Some(value) = event.data.downcast_ref::<TimerFired>() {
                events.push(McEvent::TimerFired {
                    proc: value.proc.clone(),
                    timer: value.timer.clone(),
                    timer_delay: McTime::from(0.0),
                });
            }
        }

        let mc_net = Rc::new(RefCell::new(McNetwork::new(sys.network())));

        let mut nodes: HashMap<String, McNode> = HashMap::new();
        for node in sys.nodes() {
            let node = sys.get_node(&node).unwrap();
            nodes.insert(node.name.clone(), McNode::new(node.processes(), mc_net.clone()));
        }

        Self {
            system: McSystem::new(nodes, mc_net, events),
            strategy,
        }
    }

    /// Runs model checking and returns the result on completion.
    pub fn run(&mut self) -> Result<McResult, String> {
        t!("RUNNING MODEL CHECKING THROUGH POSSIBLE EXECUTION PATHS"
            .to_string()
            .yellow());
        self.strategy.run(&mut self.system)
    }

    pub fn set_state(&mut self, mut state: McState) {
        state.search_depth = 0;
        state.events.is_insta = self.system.events.is_insta;
        self.system.set_state(state);
    }

    pub fn apply_event(&mut self, event: McEvent) {
        self.system.apply_event(event);
    }
}
