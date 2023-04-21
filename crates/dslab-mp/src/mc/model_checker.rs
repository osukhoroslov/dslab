//! Model checker configuration and launching.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use colored::*;

use crate::events::{MessageReceived, TimerFired};
use crate::mc::events::{DeliveryOptions, McEvent, McTime};
use crate::mc::network::McNetwork;
use crate::mc::node::McNode;
use crate::mc::pending_events::PendingEvents;
use crate::mc::strategy::{McSummary, Strategy};
use crate::mc::system::McSystem;
use crate::system::System;
use crate::util::t;

/// Main class of (and entrypoint to) the model checking testing technique.
pub struct ModelChecker {
    system: McSystem,
    strategy: Box<dyn Strategy>,
}

impl ModelChecker {
    /// Creates a new model checker with the specified strategy
    /// and initial state equal to the current state of the system.
    pub fn new(sys: &System, strategy: Box<dyn Strategy>) -> Self {
        let sim = sys.sim();

        let mut events = PendingEvents::new();
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
    pub fn run(&mut self) -> Result<McSummary, String> {
        t!("RUNNING MODEL CHECKING THROUGH POSSIBLE EXECUTION PATHS"
            .to_string()
            .yellow());
        self.strategy.run(&mut self.system)
    }
}
