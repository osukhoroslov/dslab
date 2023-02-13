use std::cell::{RefCell, RefMut};
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use colored::*;
use lazy_static::lazy_static;
use rand::prelude::*;
use rand_pcg::Pcg64;
use regex::Regex;

use crate::mc::events::{EventInfo, McEvent};
use crate::mc::strategy::LogMode;
use crate::message::Message;
use crate::network::Network;
use crate::util::t;

pub struct McNetwork {
    rand: Pcg64,
    corrupt_rate: f64,
    dupl_rate: f64,
    drop_rate: f64,
    drop_incoming: HashSet<String>,
    drop_outgoing: HashSet<String>,
    disabled_links: HashSet<(String, String)>,
    proc_locations: HashMap<String, String>,
    events: Rc<RefCell<Vec<McEvent>>>,
    log_mode: LogMode,
}

impl McNetwork {
    pub(crate) fn new(
        rand: Pcg64,
        net: RefMut<Network>,
        events: Rc<RefCell<Vec<McEvent>>>,
        log_mode: &LogMode,
    ) -> Self {
        Self {
            rand,
            corrupt_rate: net.corrupt_rate(),
            dupl_rate: net.dupl_rate(),
            drop_rate: net.drop_rate(),
            drop_incoming: net.get_drop_incoming().clone(),
            drop_outgoing: net.get_drop_outgoing().clone(),
            disabled_links: net.disabled_links().clone(),
            proc_locations: net.proc_locations().clone(),
            events,
            log_mode: log_mode.clone(),
        }
    }

    pub fn get_proc_node(&self, proc: &String) -> &String {
        &self.proc_locations[proc]
    }

    pub fn send_message(&mut self, msg: Message, src: String, dest: String) -> Option<EventInfo> {
        let src_node = self.get_proc_node(&src).clone();
        let dest_node = self.get_proc_node(&dest).clone();

        let receive_event = McEvent::MessageReceived { msg, src, dest };
        if src_node == dest_node {
            return Some(EventInfo {
                event: receive_event,
                can_be_dropped: false,
                can_be_duplicated: false,
                can_be_corrupted: false,
            });
        } else if !self.message_is_definitely_dropped(&src_node, &dest_node) {
            return Some(EventInfo {
                event: receive_event,
                can_be_dropped: self.drop_rate > 0.,
                can_be_duplicated: self.dupl_rate > 0.,
                can_be_corrupted: self.corrupt_rate > 0.,
            });
        }
        None
    }

    fn rand(&mut self) -> f64 {
        self.rand.gen_range(0.0..1.0)
    }

    fn message_is_dropped(&mut self, src: &String, dest: &String) -> bool {
        self.rand() < self.drop_rate
            || self.drop_outgoing.contains(src)
            || self.drop_incoming.contains(dest)
            || self.disabled_links.contains(&(src.clone(), dest.clone()))
    }

    fn message_is_definitely_dropped(&mut self, src: &String, dest: &String) -> bool {
        self.drop_outgoing.contains(src)
            || self.drop_incoming.contains(dest)
            || self.disabled_links.contains(&(src.clone(), dest.clone()))
    }

    fn corrupt_if_needed(&mut self, msg: Message) -> Message {
        if self.rand() < self.corrupt_rate {
            lazy_static! {
                static ref RE: Regex = Regex::new(r#""\w+""#).unwrap();
            }
            let corrupted_data = RE.replace_all(&msg.data, "\"\"").to_string();
            let new_msg = Message::new(msg.tip.clone(), corrupted_data);

            if self.log_mode == LogMode::Debug {
                t!(format!("{:?} => {:?} <-- message corrupted", msg, new_msg).red());
            }

            new_msg
        } else {
            msg
        }
    }

    fn get_message_count(&mut self) -> u32 {
        if self.rand() >= self.dupl_rate {
            1
        } else {
            (self.rand() * 2.).ceil() as u32 + 1
        }
    }
}
