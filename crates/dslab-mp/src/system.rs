use std::cell::{Ref, RefCell, RefMut};
use std::collections::HashMap;
use std::path::Path;
use std::rc::Rc;

use rand::distributions::uniform::{SampleRange, SampleUniform};

use dslab_core::{cast, Simulation};

use crate::events::MessageReceived;
use crate::logger::{LogEntry, Logger};
use crate::message::Message;
use crate::network::Network;
use crate::node::{EventLogEntry, Node};
use crate::process::Process;

pub struct System {
    sim: Simulation,
    net: Rc<RefCell<Network>>,
    nodes: HashMap<String, Rc<RefCell<Node>>>,
    proc_nodes: HashMap<String, Rc<RefCell<Node>>>,
    logger: Rc<RefCell<Logger>>,
}

impl System {
    pub fn new(seed: u64) -> Self {
        let logger = Rc::new(RefCell::new(Logger::new()));
        let mut sim = Simulation::new(seed);
        let net = Rc::new(RefCell::new(Network::new(sim.create_context("net"), logger.clone())));
        Self {
            sim,
            net,
            nodes: HashMap::new(),
            proc_nodes: HashMap::new(),
            logger,
        }
    }

    pub fn with_log_file(seed: u64, log_path: &Path) -> Self {
        let logger = Rc::new(RefCell::new(Logger::with_log_file(log_path)));
        let mut sim = Simulation::new(seed);
        let net = Rc::new(RefCell::new(Network::new(sim.create_context("net"), logger.clone())));
        Self {
            sim,
            net,
            nodes: HashMap::new(),
            proc_nodes: HashMap::new(),
            logger,
        }
    }

    pub fn logger(&self) -> RefMut<Logger> {
        self.logger.borrow_mut()
    }

    // Network ---------------------------------------------------------------------------------------------------------

    pub fn network(&self) -> RefMut<Network> {
        self.net.borrow_mut()
    }

    // Nodes -----------------------------------------------------------------------------------------------------------

    pub fn nodes(&self) -> Vec<String> {
        self.nodes.keys().cloned().collect()
    }

    pub fn add_node(&mut self, name: &str) {
        let node = Rc::new(RefCell::new(Node::new(
            name.to_string(),
            self.net.clone(),
            self.sim.create_context(name),
            self.logger.clone(),
        )));
        let node_id = self.sim.add_handler(name, node.clone());
        assert!(
            self.nodes.insert(name.to_string(), node).is_none(),
            "Node with name {} already exists, node names must be unique",
            name
        );
        self.net.borrow_mut().add_node(name.to_string(), node_id);
        self.logger.borrow_mut().log(LogEntry::NodeStarted {
            time: self.sim.time(),
            node: name.to_string(),
            node_id,
        });
    }

    pub fn node_names(&self) -> Vec<String> {
        self.nodes.keys().cloned().collect()
    }

    pub fn set_node_clock_skew(&mut self, node: &str, clock_skew: f64) {
        self.nodes[node].borrow_mut().set_clock_skew(clock_skew);
    }

    pub fn crash_node(&mut self, node_name: &str) {
        let node = self.nodes.get(node_name).unwrap();
        // remove the handler to discard all events sent to this node
        self.sim.remove_handler(node_name);
        node.borrow_mut().crash();

        self.logger.borrow_mut().log(LogEntry::NodeCrashed {
            time: self.sim.time(),
            node: node_name.to_string(),
        });

        // cancel pending events (i.e. undelivered messages) from the crashed node
        let node_id = self.sim.lookup_id(node_name);
        let cancelled = self.sim.cancel_and_get_events(|e| e.src == node_id);
        for event in cancelled {
            cast!(match event.data {
                MessageReceived {
                    id,
                    msg,
                    src,
                    src_node,
                    dst,
                    dst_node,
                } => {
                    self.logger.borrow_mut().log(LogEntry::MessageDropped {
                        time: self.sim.time(),
                        msg_id: id.to_string(),
                        msg,
                        src_proc: src,
                        src_node,
                        dst_proc: dst,
                        dst_node,
                    });
                }
            })
        }
    }

    pub fn recover_node(&mut self, node_name: &str) {
        let node = self.nodes.get(node_name).unwrap();
        node.borrow_mut().recover();
        self.sim.add_handler(node_name, node.clone());

        // remove previous process-node mappings to enable recreating these processes
        self.proc_nodes.retain(|_, node| node.borrow().name != node_name);

        self.logger.borrow_mut().log(LogEntry::NodeRecovered {
            time: self.sim.time(),
            node: node_name.to_string(),
        });
    }

    pub fn get_node(&self, name: &str) -> Option<Ref<Node>> {
        self.nodes.get(name).map(|res| res.borrow())
    }

    pub fn get_mut_node(&self, name: &str) -> Option<RefMut<Node>> {
        self.nodes.get(name).map(|res| res.borrow_mut())
    }

    pub fn node_is_crashed(&self, node: &str) -> bool {
        self.nodes.get(node).unwrap().borrow().is_crashed()
    }

    // Processes -------------------------------------------------------------------------------------------------------

    pub fn add_process(&mut self, name: &str, proc: Box<dyn Process>, node: &str) {
        self.nodes[node].borrow_mut().add_process(name, proc);
        self.net
            .borrow_mut()
            .set_proc_location(name.to_string(), node.to_string());
        assert!(
            self.proc_nodes
                .insert(name.to_string(), self.nodes[node].clone())
                .is_none(),
            "Process with name {} already exists, process names must be unique",
            name
        );
        self.logger.borrow_mut().log(LogEntry::ProcessStarted {
            time: self.sim.time(),
            node: node.to_string(),
            proc: name.to_string(),
        });
    }

    pub fn process_names(&self) -> Vec<String> {
        self.proc_nodes.keys().cloned().collect()
    }

    pub fn send_local_message(&mut self, proc: &str, msg: Message) {
        let mut node = self.proc_nodes[proc].borrow_mut();
        assert!(
            !node.is_crashed(),
            "Cannot send local message to process {} on crashed node {}",
            proc,
            node.name
        );
        node.send_local_message(proc.to_string(), msg);
    }

    pub fn read_local_messages(&mut self, proc: &str) -> Vec<Message> {
        self.proc_nodes[proc]
            .borrow_mut()
            .read_local_messages(proc)
            .unwrap_or_default()
    }

    pub fn local_outbox(&self, proc: &str) -> Vec<Message> {
        self.proc_nodes[proc].borrow().local_outbox(proc)
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

    pub fn proc_node_name(&self, proc: &str) -> String {
        self.proc_nodes[proc].borrow().name().to_owned()
    }

    pub fn proc_node_is_crashed(&self, proc: &str) -> bool {
        self.proc_nodes[proc].borrow().is_crashed()
    }

    // Simulation ------------------------------------------------------------------------------------------------------

    pub fn sim(&self) -> &Simulation {
        &self.sim
    }

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
        loop {
            if let Some(messages) = node.borrow_mut().read_local_messages(proc) {
                return Ok(messages);
            }
            if !self.step() {
                return Err("No messages");
            }
        }
    }

    pub fn step_until_local_message_max_steps(&mut self, proc: &str, max_steps: u32) -> Result<Vec<Message>, &str> {
        let mut steps = 0;
        let node = self.proc_nodes[proc].clone();
        while steps < max_steps {
            if let Some(messages) = node.borrow_mut().read_local_messages(proc) {
                return Ok(messages);
            }
            if !self.step() {
                break;
            }
            steps += 1;
        }
        Err("No messages")
    }

    pub fn step_until_local_message_timeout(&mut self, proc: &str, timeout: f64) -> Result<Vec<Message>, &str> {
        let end_time = self.time() + timeout;
        let node = self.proc_nodes[proc].clone();
        while self.time() < end_time {
            if let Some(messages) = node.borrow_mut().read_local_messages(proc) {
                return Ok(messages);
            }
            if !self.step() {
                break;
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
