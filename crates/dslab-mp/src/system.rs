use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use colored::*;
use rand::distributions::uniform::{SampleRange, SampleUniform};

use dslab_core::Simulation;

use crate::message::Message;
use crate::network::Network;
use crate::node::{EventLogEntry, Node};
use crate::process::Process;
use crate::util::t;

pub struct System {
    sim: Simulation,
    net: Rc<RefCell<Network>>,
    nodes: HashMap<String, Rc<RefCell<Node>>>,
    proc_nodes: HashMap<String, Rc<RefCell<Node>>>,
}

impl System {
    pub fn new(seed: u64) -> Self {
        let mut sim = Simulation::new(seed);
        let net = Rc::new(RefCell::new(Network::new(sim.create_context("net"))));
        Self {
            sim,
            net,
            nodes: HashMap::new(),
            proc_nodes: HashMap::new(),
        }
    }

    // Network ---------------------------------------------------------------------------------------------------------

    pub fn network(&self) -> &Rc<RefCell<Network>> {
        &self.net
    }

    // Nodes -----------------------------------------------------------------------------------------------------------

    pub fn add_node(&mut self, name: &str) {
        let node = Rc::new(RefCell::new(Node::new(
            name.to_string(),
            self.net.clone(),
            self.sim.create_context(name),
        )));
        let node_id = self.sim.add_handler(name, node.clone());
        self.nodes.insert(name.to_string(), node);
        self.net.borrow_mut().add_node(name.to_string(), node_id);
        t!(format!("{:>9.3} - node started: {}", self.sim.time(), name));
    }

    pub fn set_node_clock_skew(&mut self, node: &str, clock_skew: f64) {
        self.nodes[node].borrow_mut().set_clock_skew(clock_skew);
    }

    pub fn crash_node(&mut self, node: &str) {
        self.sim.remove_handler(node);

        // cancel pending events from the crashed node
        let node_id = self.sim.lookup_id(node);
        self.sim.cancel_events(|e| e.src == node_id);

        t!(format!("{:>9.3} - node crashed: {}", self.sim.time(), node).red());
    }

    // Processes -------------------------------------------------------------------------------------------------------

    pub fn add_process(&mut self, name: &str, proc: Box<dyn Process>, node: &str) {
        self.nodes[node].borrow_mut().add_process(name, proc);
        self.net
            .borrow_mut()
            .set_proc_location(name.to_string(), node.to_string());
        self.proc_nodes.insert(name.to_string(), self.nodes[node].clone());
        t!(format!(
            "{:>9.3} - process started: {} @ {}",
            self.sim.time(),
            name,
            node
        ));
    }

    pub fn process_names(&self) -> Vec<String> {
        self.proc_nodes.keys().cloned().collect()
    }

    pub fn send_local_message(&mut self, proc: &str, msg: Message) {
        self.proc_nodes[proc]
            .borrow_mut()
            .send_local_message(proc.to_string(), msg);
    }

    pub fn read_local_messages(&mut self, proc: &str) -> Vec<Message> {
        self.proc_nodes[proc]
            .borrow_mut()
            .read_local_messages(proc)
            .unwrap_or_default()
    }

    pub fn event_log(&self, proc: &str) -> Vec<EventLogEntry> {
        self.proc_nodes[proc].borrow().event_log(proc)
    }

    pub fn max_size(&mut self, proc: &str) -> u64 {
        self.proc_nodes[proc].borrow_mut().max_size(proc)
    }

    pub fn sent_message_count(&self, proc: &str) -> u64 {
        self.proc_nodes[proc].borrow().sent_message_count(proc)
    }

    pub fn received_message_count(&self, proc: &str) -> u64 {
        self.proc_nodes[proc].borrow().received_message_count(proc)
    }

    // Simulation ------------------------------------------------------------------------------------------------------

    pub fn time(&self) -> f64 {
        self.sim.time()
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
        let node = self.proc_nodes[proc].clone();
        while self.step() {
            if let Some(messages) = node.borrow_mut().read_local_messages(proc) {
                return Ok(messages);
            }
        }
        Err("No messages")
    }

    pub fn step_until_local_message_max_steps(&mut self, proc: &str, max_steps: u32) -> Result<Vec<Message>, &str> {
        let mut steps = 0;
        let node = self.proc_nodes[proc].clone();
        while self.step() && steps <= max_steps {
            if let Some(messages) = node.borrow_mut().read_local_messages(proc) {
                return Ok(messages);
            }
            steps += 1;
        }
        Err("No messages")
    }

    pub fn step_until_local_message_timeout(&mut self, proc: &str, timeout: f64) -> Result<Vec<Message>, &str> {
        let end_time = self.time() + timeout;
        let node = self.proc_nodes[proc].clone();
        while self.step() && self.time() < end_time {
            if let Some(messages) = node.borrow_mut().read_local_messages(proc) {
                return Ok(messages);
            }
        }
        Err("No messages")
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
}
