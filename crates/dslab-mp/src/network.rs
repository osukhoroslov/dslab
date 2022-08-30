use std::collections::{HashMap, HashSet};

use colored::*;

use dslab_core::Id;
use dslab_core::SimulationContext;

use crate::events::MessageReceived;
use crate::message::Message;
use crate::util::t;

pub struct Network {
    min_delay: f64,
    max_delay: f64,
    drop_rate: f64,
    dupl_rate: f64,
    corrupt_rate: f64,
    node_ids: HashMap<String, Id>,
    proc_nodes: HashMap<String, String>,
    crashed_nodes: HashSet<String>,
    drop_incoming: HashSet<String>,
    drop_outgoing: HashSet<String>,
    disabled_links: HashSet<(String, String)>,
    message_count: u64,
    traffic: u64,
    ctx: SimulationContext,
}

impl Network {
    pub fn new(ctx: SimulationContext) -> Self {
        Self {
            min_delay: 1.,
            max_delay: 1.,
            drop_rate: 0.,
            dupl_rate: 0.,
            corrupt_rate: 0.,
            node_ids: HashMap::new(),
            proc_nodes: HashMap::new(),
            crashed_nodes: HashSet::new(),
            drop_incoming: HashSet::new(),
            drop_outgoing: HashSet::new(),
            disabled_links: HashSet::new(),
            message_count: 0,
            traffic: 0,
            ctx,
        }
    }

    pub fn add_node(&mut self, name: String, id: Id) {
        self.node_ids.insert(name, id);
    }

    pub fn set_proc_location(&mut self, proc: String, node: String) {
        self.proc_nodes.insert(proc, node);
    }

    pub fn get_proc_location(&self, proc: String) -> String {
        self.proc_nodes.get(&proc).unwrap().clone()
    }

    pub fn set_delay(&mut self, delay: f64) {
        self.min_delay = delay;
        self.max_delay = delay;
    }

    pub fn set_delays(&mut self, min_delay: f64, max_delay: f64) {
        self.min_delay = min_delay;
        self.max_delay = max_delay;
    }

    pub fn set_drop_rate(&mut self, drop_rate: f64) {
        self.drop_rate = drop_rate;
    }

    pub fn set_dupl_rate(&mut self, dupl_rate: f64) {
        self.dupl_rate = dupl_rate;
    }

    pub fn set_corrupt_rate(&mut self, corrupt_rate: f64) {
        self.corrupt_rate = corrupt_rate;
    }

    pub fn node_crashed(&mut self, node_id: &str) {
        self.crashed_nodes.insert(node_id.to_string());
    }

    pub fn node_recovered(&mut self, node_id: &str) {
        self.crashed_nodes.remove(node_id);
    }

    pub fn drop_incoming(&mut self, node_id: &str) {
        self.drop_incoming.insert(node_id.to_string());
    }

    pub fn pass_incoming(&mut self, node_id: &str) {
        self.drop_incoming.remove(node_id);
    }

    pub fn drop_outgoing(&mut self, node_id: &str) {
        self.drop_outgoing.insert(node_id.to_string());
    }

    pub fn pass_outgoing(&mut self, node_id: &str) {
        self.drop_outgoing.remove(node_id);
    }

    pub fn disconnect_node(&mut self, node_id: &str) {
        self.drop_incoming.insert(node_id.to_string());
        self.drop_outgoing.insert(node_id.to_string());
    }

    pub fn connect_node(&mut self, node_id: &str) {
        self.drop_incoming.remove(node_id);
        self.drop_outgoing.remove(node_id);
    }

    pub fn disable_link(&mut self, from: &str, to: &str) {
        self.disabled_links.insert((from.to_string(), to.to_string()));
    }

    pub fn enable_link(&mut self, from: &str, to: &str) {
        self.disabled_links.remove(&(from.to_string(), to.to_string()));
    }

    pub fn make_partition(&mut self, group1: &[&str], group2: &[&str]) {
        for n1 in group1 {
            for n2 in group2 {
                self.disabled_links.insert((n1.to_string(), n2.to_string()));
                self.disabled_links.insert((n2.to_string(), n1.to_string()));
            }
        }
    }

    pub fn reset_network(&mut self) {
        self.disabled_links.clear();
        self.drop_incoming.clear();
        self.drop_outgoing.clear();
    }

    pub fn get_message_count(&self) -> u64 {
        self.message_count
    }

    pub fn get_traffic(&self) -> u64 {
        self.traffic
    }

    pub fn send_message(&mut self, msg: Message, src: String, dest: String) {
        //let msg_size = msg.size();
        let src_node = self.proc_nodes.get(&src).unwrap();
        let dest_node = self.proc_nodes.get(&dest).unwrap();
        let dest_node_id = *self.node_ids.get(dest_node).unwrap();
        if !self.crashed_nodes.contains(src_node) {
            if self.ctx.rand() >= self.drop_rate
                && !self.drop_outgoing.contains(src_node)
                && !self.drop_incoming.contains(dest_node)
                && !self.disabled_links.contains(&(src_node.clone(), dest_node.clone()))
            {
                let delay = self.min_delay + self.ctx.rand() * (self.max_delay - self.min_delay);
                if self.ctx.rand() < self.corrupt_rate {
                    // TODO: support message corruption
                }
                let e = MessageReceived {
                    msg,
                    src,
                    dest: dest.clone(),
                };
                if self.ctx.rand() >= self.dupl_rate {
                    self.ctx.emit(e, dest_node_id, delay);
                } else {
                    let dups = (self.ctx.rand() * 2.).ceil() as u32 + 1;
                    for _i in 0..dups {
                        self.ctx.emit(e.clone(), dest_node_id, delay);
                    }
                }
            } else {
                t!(format!(
                    "{:>9} {:>10} --x {:<10} {:?} <-- message dropped",
                    "!!!", src, dest, msg
                )
                .yellow());
            }
        } else {
            t!(format!("Discarded message from crashed node {:?}", msg).yellow());
        }
        self.message_count += 1;
        //self.traffic += msg_size;
    }
}

// impl EventHandler for Network {
//     fn on(&mut self, event: Event) {
//         cast!(match event.data {
//             MessageSent { msg, src, dest } => {
//                 self.on_message_sent(msg, src, dest);
//             }
//         })
//     }
// }
