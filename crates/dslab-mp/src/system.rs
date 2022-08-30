use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use dslab_core::Simulation;

use crate::message::Message;
use crate::network::Network;
use crate::node::{EventLogEntry, Node};
use crate::process::Process;

pub struct System {
    sim: Simulation,
    net: Rc<RefCell<Network>>,
    nodes: HashMap<String, Rc<RefCell<Node>>>,
}

impl System {
    pub fn new(seed: u64) -> Self {
        let mut sim = Simulation::new(seed);
        let net = Rc::new(RefCell::new(Network::new(sim.create_context("net"))));
        //sim.add_handler("net", net.clone());
        Self {
            sim,
            net,
            nodes: HashMap::new(),
        }
    }

    pub fn add_node<T>(&mut self, name: T)
    where
        T: Into<String>,
    {
        let node_name = name.into();
        let node = Rc::new(RefCell::new(Node::new(
            self.net.clone(),
            self.sim.create_context(node_name.clone()),
        )));
        let node_id = self.sim.add_handler(node_name.clone(), node.clone());
        self.nodes.insert(node_name.clone(), node);
        self.net.borrow_mut().add_node(node_name.clone(), node_id);
    }

    pub fn add_process<T>(&mut self, proc: Rc<RefCell<dyn Process>>, proc_name: T, node_name: T)
    where
        T: Into<String>,
    {
        let proc_name = proc_name.into();
        let node_name = node_name.into();
        let node = self.nodes.get(&node_name).unwrap();
        node.borrow_mut().add_proc(proc, proc_name.clone());
        self.net.borrow_mut().set_proc_location(proc_name.clone(), node_name);
    }

    pub fn send_local(&mut self, msg: Message, proc: &str) {
        let proc_node = self.net.borrow().get_proc_location(proc.to_string());
        self.nodes
            .get(&proc_node)
            .unwrap()
            .borrow_mut()
            .send_local(msg, proc.to_string());
    }

    pub fn event_log(&self, proc: &str) -> Vec<EventLogEntry> {
        let proc_node = self.net.borrow().get_proc_location(proc.to_string());
        self.nodes.get(&proc_node).unwrap().borrow().event_log(proc.to_string())
    }

    pub fn network(&self) -> Rc<RefCell<Network>> {
        self.net.clone()
    }

    pub fn step(&mut self) -> bool {
        self.sim.step()
    }

    pub fn steps(&mut self, step_count: u64) -> bool {
        self.sim.steps(step_count)
    }

    pub fn step_until_no_events(&mut self) {
        self.sim.step_until_no_events()
    }

    pub fn step_for_duration(&mut self, duration: f64) {
        self.sim.step_for_duration(duration)
    }

    pub fn step_until_local_message(&mut self, proc: &str) -> Result<Vec<Message>, &str> {
        while self.step() {
            match self.read_local_messages(proc) {
                Some(messages) => return Ok(messages),
                None => (),
            }
        }
        Err("No messages")
    }
    //
    // pub fn step_until_local_message_max_steps(&mut self, node_id: &str, max_steps: u32) -> Result<Vec<M>, &str> {
    //     let mut steps = 0;
    //     while self.step() && steps <= max_steps {
    //         match self.check_mailbox(node_id) {
    //             Some(messages) => return Ok(messages),
    //             None => (),
    //         }
    //         steps += 1;
    //     }
    //     Err("No messages")
    // }

    pub fn read_local_messages(&mut self, proc: &str) -> Option<Vec<Message>> {
        let proc = proc.to_string();
        let proc_node = self.net.borrow().get_proc_location(proc.clone());
        self.nodes
            .get(&proc_node)
            .unwrap()
            .borrow_mut()
            .read_local_messages(proc)
    }
}
