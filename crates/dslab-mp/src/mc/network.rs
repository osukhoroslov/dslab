use std::cell::RefMut;
use std::collections::{HashMap, HashSet};

use crate::mc::events::{DeliveryOptions, McEvent, McTime};
use crate::message::Message;
use crate::network::Network;

const DUPL_COUNT: u32 = 2;

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

    pub fn send_message(&mut self, msg: Message, src: String, dest: String) -> McEvent {
        let src_node = self.get_proc_node(&src).clone();
        let dest_node = self.get_proc_node(&dest).clone();

        if src_node == dest_node {
            McEvent::MessageReceived {
                msg,
                src,
                dest,
                options: DeliveryOptions::NoFailures(McTime::from(self.max_delay)),
            }
        } else if !self.drop_outgoing.contains(&src_node)
            && !self.drop_incoming.contains(&dest_node)
            && !self.disabled_links.contains(&(src_node, dest_node))
        {
            McEvent::MessageReceived {
                msg,
                src,
                dest,
                options: DeliveryOptions::PossibleFailures {
                    can_be_dropped: self.drop_rate > 0.,
                    max_dupl_count: if self.dupl_rate == 0. { 0 } else { DUPL_COUNT },
                    can_be_corrupted: self.corrupt_rate > 0.,
                },
            }
        } else {
            McEvent::MessageDropped { msg, src, dest }
        }
    }
}
