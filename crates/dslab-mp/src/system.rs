use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use colored::*;
use rand::distributions::uniform::{SampleRange, SampleUniform};

use dslab_core::Simulation;

use crate::message::Message;
use crate::network::Network;
use crate::node::{EventLogEntry, Node};
use crate::util::t;

pub struct System {
    sim: Simulation,
    net: Rc<RefCell<Network>>,
    nodes: HashMap<String, Rc<RefCell<Node>>>,
}

impl System {
    pub fn new(seed: u64) -> Self {
        let mut sim = Simulation::new(seed);
        let net = Rc::new(RefCell::new(Network::new(sim.create_context("net"))));
        Self {
            sim,
            net,
            nodes: HashMap::new(),
        }
    }

    pub fn add_node<S>(&mut self, node_name: S) -> Rc<RefCell<Node>>
    where
        S: AsRef<str>,
    {
        let node = Rc::new(RefCell::new(Node::new(
            node_name.as_ref().to_string(),
            self.net.clone(),
            self.sim.create_context(node_name.as_ref()),
        )));
        let node_id = self.sim.add_handler(node_name.as_ref(), node.clone());
        self.nodes.insert(node_name.as_ref().to_string(), node.clone());
        self.net.borrow_mut().add_node(node_name.as_ref().to_string(), node_id);
        t!(format!("{:>9.3} {:>10} STARTED", self.sim.time(), node_name.as_ref())
            .green()
            .bold());
        node
    }

    pub fn node<S>(&self, node_name: S) -> Rc<RefCell<Node>>
    where
        S: AsRef<str>,
    {
        self.nodes.get(node_name.as_ref()).unwrap().clone()
    }

    pub fn crash_node<S>(&mut self, node_name: S)
    where
        S: AsRef<str>,
    {
        self.sim.remove_handler(node_name.as_ref());

        // cancel pending events from the crashed node
        let node_id = self.sim.lookup_id(node_name.as_ref());
        self.sim.cancel_events(|e| e.src == node_id);

        t!(format!("{:>9.3} {:>10} CRASHED", self.sim.time(), node_name.as_ref())
            .red()
            .bold());
    }

    pub fn process_names(&self) -> Vec<String> {
        self.net.borrow().process_names()
    }

    pub fn send_local(&mut self, msg: Message, proc: &str) {
        let proc_node = self.net.borrow().proc_location(proc.to_string());
        self.nodes
            .get(&proc_node)
            .unwrap()
            .borrow_mut()
            .send_local(msg, proc.to_string());
    }

    pub fn event_log(&self, proc: &str) -> Vec<EventLogEntry> {
        let proc_node = self.net.borrow().proc_location(proc.to_string());
        self.nodes.get(&proc_node).unwrap().borrow().event_log(proc.to_string())
    }

    pub fn network(&self) -> Rc<RefCell<Network>> {
        self.net.clone()
    }

    pub fn time(&self) -> f64 {
        self.sim.time()
    }

    pub fn gen_range<T, R>(&mut self, range: R) -> T
    where
        T: SampleUniform,
        R: SampleRange<T>,
    {
        self.sim.gen_range(range)
    }

    pub fn random_string(&mut self, len: usize) -> String {
        self.sim.random_string(len)
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

    pub fn step_for_duration(&mut self, duration: f64) -> bool {
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

    pub fn step_until_local_message_max_steps(&mut self, proc: &str, max_steps: u32) -> Result<Vec<Message>, &str> {
        let mut steps = 0;
        while self.step() && steps <= max_steps {
            match self.read_local_messages(proc) {
                Some(messages) => return Ok(messages),
                None => (),
            }
            steps += 1;
        }
        Err("No messages")
    }

    pub fn step_until_local_message_with_timeout(&mut self, proc: &str, timeout: f64) -> Result<Vec<Message>, &str> {
        let end_time = self.time() + timeout;
        while self.step() && self.time() < end_time {
            match self.read_local_messages(proc) {
                Some(messages) => return Ok(messages),
                None => (),
            }
        }
        Err("No messages")
    }

    pub fn read_local_messages(&mut self, proc: &str) -> Option<Vec<Message>> {
        let proc = proc.to_string();
        let proc_node = self.net.borrow().proc_location(proc.clone());
        self.nodes
            .get(&proc_node)
            .unwrap()
            .borrow_mut()
            .read_local_messages(proc)
    }
}
