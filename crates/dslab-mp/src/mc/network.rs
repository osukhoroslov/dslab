use std::cell::{RefCell, RefMut};
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use colored::*;
use lazy_static::lazy_static;
use rand::prelude::*;
use rand_pcg::Pcg64;
use regex::Regex;

use crate::mc::events::McEvent;
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
    mode: LogMode,
}

impl McNetwork {
    pub(crate) fn new(rand: Pcg64, net: RefMut<Network>, events: Rc<RefCell<Vec<McEvent>>>, mode: LogMode) -> Self {
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
            mode,
        }
    }

    pub fn get_proc_node(&self, proc: &String) -> &String {
        &self.proc_locations[proc]
    }

    pub fn send_message(&mut self, msg: Message, src: String, dest: String) {
        let src_node = self.get_proc_node(&src).clone();
        let dest_node = self.get_proc_node(&dest).clone();

        if src_node != dest_node && self.message_is_dropped(&src_node, &dest_node) {
            if let LogMode::Debug = self.mode {
                t!(format!(
                    "{:>9} {:>10} --x {:<10} {:?} <-- message dropped",
                    "!!!", src, dest, msg
                )
                .red());
            }
            return;
        }

        let msg = self.corrupt_if_needed(msg);

        let msg_count = self.get_message_count();

        if let LogMode::Debug = self.mode {
            t!("x{:<8} {:>10} --> {:<10} {:?}", msg_count, src, dest, msg);
        }

        let data = McEvent::MessageReceived { msg, src, dest };
        if msg_count == 1 {
            self.events.borrow_mut().push(data);
        } else {
            for _ in 0..msg_count {
                self.events.borrow_mut().push(data.clone());
            }
        }
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

    fn corrupt_if_needed(&mut self, msg: Message) -> Message {
        if self.rand() < self.corrupt_rate {
            lazy_static! {
                static ref RE: Regex = Regex::new(r#""\w+""#).unwrap();
            }
            let corrupted_data = RE.replace_all(&msg.data, "\"\"").to_string();
            let new_msg = Message::new(msg.tip.clone(), corrupted_data);

            if let LogMode::Debug = self.mode {
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
