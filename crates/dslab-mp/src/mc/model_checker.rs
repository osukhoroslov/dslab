use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use crate::events::{MessageReceived, TimerFired};
use crate::mc::events::{DeliveryOptions, McEvent};
use crate::mc::network::McNetwork;
use crate::mc::node::McNode;
use crate::mc::strategy::McSummary;
use crate::mc::strategy::Strategy;
use crate::mc::system::McSystem;
use crate::system::System;

use super::events::McTime;
use super::pending_events::PendingEvents;

pub struct ModelChecker {
    system: McSystem,
    strategy: Box<dyn Strategy>,
}

impl ModelChecker {
    pub fn new(sys: &System, strategy: Box<dyn Strategy>) -> Self {
        let sim = sys.sim();

        let mut events = PendingEvents::new();
        for event in sim.state().events() {
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

    pub fn start(&mut self) -> Result<McSummary, String> {
        self.strategy.run(&mut self.system)
    }
}
