use std::cell::RefMut;
use std::collections::{HashMap, HashSet};

use serde::Serialize;

use crate::mc::events::McEvent;
use crate::mc::system::McTime;
use crate::message::Message;
use crate::network::Network;

const DUPL_COUNT: u32 = 2;

#[derive(Serialize, Clone, PartialEq, Eq, Hash, Debug)]
pub enum DeliveryOptions {
    /// Message will be received exactly once without corruption with specified max delay
    NoFailures(McTime),
    /// Message delivery may be subject to some failures
    PossibleFailures {
        can_be_dropped: bool,
        max_dupl_count: u32,
        can_be_corrupted: bool,
    },
}

pub struct McNetwork {
    corrupt_rate: f64,
    dupl_rate: f64,
    drop_rate: f64,
    drop_incoming: HashSet<String>,
    drop_outgoing: HashSet<String>,
    disabled_links: HashSet<(String, String)>,
    proc_locations: HashMap<String, String>,
    max_delay: f64,
}

impl McNetwork {
    pub(crate) fn new(net: RefMut<Network>) -> Self {
        Self {
            corrupt_rate: net.corrupt_rate(),
            dupl_rate: net.dupl_rate(),
            drop_rate: net.drop_rate(),
            drop_incoming: net.get_drop_incoming().clone(),
            drop_outgoing: net.get_drop_outgoing().clone(),
            disabled_links: net.disabled_links().clone(),
            proc_locations: net.proc_locations().clone(),
            max_delay: net.max_delay(),
        }
    }

    pub fn get_proc_node(&self, proc: &String) -> &String {
        &self.proc_locations[proc]
    }

    pub fn send_message(&mut self, msg: Message, src: String, dst: String) -> McEvent {
        let src_node = self.get_proc_node(&src).clone();
        let dst_node = self.get_proc_node(&dst).clone();

        if src_node == dst_node {
            McEvent::MessageReceived {
                msg,
                src,
                dst,
                options: DeliveryOptions::NoFailures(McTime::from(self.max_delay)),
            }
        } else if !self.drop_outgoing.contains(&src_node)
            && !self.drop_incoming.contains(&dst_node)
            && !self.disabled_links.contains(&(src_node, dst_node))
        {
            McEvent::MessageReceived {
                msg,
                src,
                dst,
                options: DeliveryOptions::PossibleFailures {
                    can_be_dropped: self.drop_rate > 0.,
                    max_dupl_count: if self.dupl_rate == 0. { 0 } else { DUPL_COUNT },
                    can_be_corrupted: self.corrupt_rate > 0.,
                },
            }
        } else {
            McEvent::MessageDropped {
                msg,
                src,
                dst,
                receive_event_id: None,
            }
        }
    }

    pub fn drop_outgoing(&mut self, node: &str) {
        self.drop_outgoing.insert(node.to_string());
    }

    pub fn drop_incoming(&mut self, node: &str) {
        self.drop_incoming.insert(node.to_string());
    }

    pub fn disable_link(&mut self, from: &str, to: &str) {
        self.disabled_links.insert((from.to_string(), to.to_string()));
    }

    pub fn set_corrupt_rate(&mut self, corrupt_rate: f64) {
        self.corrupt_rate = corrupt_rate;
    }

    pub fn set_dupl_rate(&mut self, dupl_rate: f64) {
        self.dupl_rate = dupl_rate;
    }

    pub fn set_drop_rate(&mut self, drop_rate: f64) {
        self.drop_rate = drop_rate;
    }

    pub fn max_delay(&self) -> f64 {
        self.max_delay
    }
}
