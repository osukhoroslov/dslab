use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

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

        let mut nodes: HashMap<String, Rc<RefCell<McNode>>> = HashMap::new();
        for (name, node_cell) in sys.nodes() {
            let node = node_cell.borrow();
            nodes.insert(
                (*name).clone(),
                Rc::new(RefCell::new(McNode::new(
                    node.id(),
                    node.name(),
                    node.processes(),
                    sys.network().clone(),
                    events.clone(),
                    event_count.clone(),
                ))),
            );
        }

        Self {
            system: Rc::new(RefCell::new(McSystem::new(
                sys.network().clone(),
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
