use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use crate::mc::network::McNetwork;

use crate::mc::strategy::Strategy;

use crate::mc::node::McNode;
use crate::mc::system::McSystem;
use crate::system::System;

pub struct ModelChecker {
    system: Rc<RefCell<McSystem>>,
    strategy: Box<dyn Strategy>,
}

impl ModelChecker {
    pub fn new(sys: &System, strategy: Box<dyn Strategy>) -> Self {
        let sim = sys.sim();

        let proc_names = Rc::new(RefCell::new((*sim.names().borrow()).clone()));

        let sim_state = sim.sim_state();

        let events = Rc::new(RefCell::new(Vec::new()));
        for event in sim_state.borrow().events() {
            events.borrow_mut().push(event);
        }

        let event_count = Rc::new(RefCell::new(sim_state.borrow().event_count().clone()));

        let net = sys.network().borrow();
        let mc_net = Rc::new(RefCell::new(McNetwork::new(
            sim_state.borrow().clone_rand(),
            net.corrupt_rate(),
            net.dupl_rate(),
            net.drop_rate(),
            net.get_drop_incoming().clone(),
            net.get_drop_outgoing().clone(),
            net.disabled_links().clone(),
            net.proc_locations().clone(),
            net.node_ids().clone(),
        )));

        let mut nodes: HashMap<String, Rc<RefCell<McNode>>> = HashMap::new();
        for (name, node_cell) in sys.nodes() {
            let node = node_cell.borrow();
            nodes.insert(
                (*name).clone(),
                Rc::new(RefCell::new(McNode::new(
                    node.id(),
                    node.name(),
                    node.processes(),
                    events.clone(),
                    event_count.clone(),
                    mc_net.clone(),
                ))),
            );
        }

        Self {
            system: Rc::new(RefCell::new(McSystem::new(
                nodes,
                proc_names,
                events,
                event_count,
            ))),
            strategy,
        }
    }

    pub fn start(&mut self) -> bool {
        self.strategy.run(self.system.clone())
    }
}
