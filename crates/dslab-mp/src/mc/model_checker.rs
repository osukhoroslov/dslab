use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use crate::events::{MessageReceived, TimerFired};
use crate::mc::events::McEvent;
use crate::mc::network::McNetwork;
use crate::mc::node::McNode;
use crate::mc::strategy::Strategy;
use crate::mc::system::McSystem;
use crate::system::System;

pub struct ModelChecker {
    system: McSystem,
    strategy: Box<dyn Strategy>,
}

impl ModelChecker {
    pub fn new(sys: &System, strategy: Box<dyn Strategy>) -> Self {
        let sim = sys.sim();

        let mut events: Vec<McEvent> = Vec::new();
        for event in sim.state().events() {
            if let Some(value) = event.data.downcast_ref::<MessageReceived>() {
                events.push(McEvent::MessageReceived {
                    msg: value.msg.clone(),
                    src: value.src.clone(),
                    dest: value.dest.clone(),
                });
            } else if let Some(value) = event.data.downcast_ref::<TimerFired>() {
                events.push(McEvent::TimerFired {
                    proc: value.proc.clone(),
                    timer: value.timer.clone(),
                });
            }
        }
        let events = Rc::new(RefCell::new(events));

        let net = sys.network();
        let mc_net = Rc::new(RefCell::new(McNetwork::new(
            sim.state().rand_copy(),
            net.corrupt_rate(),
            net.dupl_rate(),
            net.drop_rate(),
            net.get_drop_incoming().clone(),
            net.get_drop_outgoing().clone(),
            net.disabled_links().clone(),
            net.proc_locations().clone(),
            events.clone(),
        )));

        let mut nodes: HashMap<String, McNode> = HashMap::new();
        for node in sys.nodes() {
            let node = sys.get_node(&node).unwrap();
            nodes.insert(
                node.name.clone(),
                McNode::new(node.processes(), mc_net.clone(), events.clone()),
            );
        }

        Self {
            system: McSystem::new(nodes, mc_net, events),
            strategy,
        }
    }

    pub fn start(&mut self) -> bool {
        self.strategy.run(&mut self.system)
    }
}
