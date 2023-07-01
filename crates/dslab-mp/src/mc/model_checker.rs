//! Model checker configuration and launching.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use colored::*;

use crate::events::{MessageReceived, TimerFired};
use crate::mc::error::McError;
use crate::mc::events::McEvent;
use crate::mc::network::McNetwork;
use crate::mc::node::McNode;
use crate::mc::pending_events::PendingEvents;
use crate::mc::strategy::{McSummary, Strategy};
use crate::mc::system::{McSystem, McTime};
use crate::mc::trace_handler::TraceHandler;
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

        let mc_net = Rc::new(RefCell::new(McNetwork::new(sys.network())));

        let trace_handler = Rc::new(RefCell::new(TraceHandler::new(sys.trace())));

        let mut nodes: HashMap<String, McNode> = HashMap::new();
        for node in sys.nodes() {
            let node = sys.get_node(&node).unwrap();
            nodes.insert(
                node.name.clone(),
                McNode::new(
                    node.processes(),
                    mc_net.clone(),
                    trace_handler.clone(),
                    node.clock_skew(),
                ),
            );
        }

        let mut events = PendingEvents::new();
        for event in sim.dump_events() {
            if let Some(value) = event.data.downcast_ref::<MessageReceived>() {
                events.push(
                    mc_net
                        .borrow_mut()
                        .send_message(value.msg.clone(), value.src.clone(), value.dest.clone()),
                );
            } else if let Some(value) = event.data.downcast_ref::<TimerFired>() {
                events.push(McEvent::TimerFired {
                    proc: value.proc.clone(),
                    timer: value.timer.clone(),
                    timer_delay: McTime::from(0.0),
                });
            }
        }

        Self {
            system: McSystem::new(nodes, mc_net, events, trace_handler),
            strategy,
        }
    }

    /// Runs model checking and returns the result on completion.
    pub fn run(&mut self) -> Result<McSummary, McError> {
        t!("RUNNING MODEL CHECKING THROUGH POSSIBLE EXECUTION PATHS"
            .to_string()
            .yellow());
        self.strategy.mark_visited(self.system.get_state());
        self.strategy.run(&mut self.system)
    }
}
