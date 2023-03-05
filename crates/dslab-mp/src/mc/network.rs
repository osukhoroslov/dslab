use std::cell::RefMut;
use std::collections::{HashMap, HashSet};

use colored::*;

use crate::mc::events::{DeliveryOptions, McEvent};
use crate::mc::strategy::LogMode;
use crate::message::Message;
use crate::network::Network;
use crate::util::t;

const DUPL_COUNT: u32 = 2;

pub struct McNetwork {
    corrupt_rate: f64,
    dupl_rate: f64,
    drop_rate: f64,
    drop_incoming: HashSet<String>,
    drop_outgoing: HashSet<String>,
    disabled_links: HashSet<(String, String)>,
    proc_locations: HashMap<String, String>,
    log_mode: LogMode,
}

impl McNetwork {
    pub(crate) fn new(net: RefMut<Network>, log_mode: &LogMode) -> Self {
        Self {
            corrupt_rate: net.corrupt_rate(),
            dupl_rate: net.dupl_rate(),
            drop_rate: net.drop_rate(),
            drop_incoming: net.get_drop_incoming().clone(),
            drop_outgoing: net.get_drop_outgoing().clone(),
            disabled_links: net.disabled_links().clone(),
            proc_locations: net.proc_locations().clone(),
            log_mode: log_mode.clone(),
        }
    }

    pub fn get_proc_node(&self, proc: &String) -> &String {
        &self.proc_locations[proc]
    }

    pub fn send_message(&mut self, msg: Message, src: String, dest: String) -> Option<McEvent> {
        let src_node = self.get_proc_node(&src).clone();
        let dest_node = self.get_proc_node(&dest).clone();

        return if src_node == dest_node {
            Some(McEvent::MessageReceived {
                msg,
                src,
                dest,
                options: DeliveryOptions::NoFailures,
            })
        } else if !self.drop_outgoing.contains(&src_node)
            && !self.drop_incoming.contains(&dest_node)
            && !self.disabled_links.contains(&(src_node, dest_node))
        {
            Some(McEvent::MessageReceived {
                msg,
                src,
                dest,
                options: DeliveryOptions::PossibleFailures {
                    can_be_dropped: self.drop_rate > 0.,
                    max_dupl_count: if self.dupl_rate == 0. { 0 } else { DUPL_COUNT },
                    can_be_corrupted: self.corrupt_rate > 0.,
                },
            })
        } else {
            if self.log_mode == LogMode::Debug {
                t!(format!(
                    "{:>9} {:>10} --x {:<10} {:?} <-- message dropped",
                    "!!!", src, dest, msg
                )
                .red());
            }
            None
        };
    }
}
