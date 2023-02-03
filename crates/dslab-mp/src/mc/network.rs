use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use lazy_static::lazy_static;
use rand::prelude::*;
use rand_pcg::Pcg64;
use regex::Regex;

use crate::mc::events::McEvent;
use crate::message::Message;

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
}

impl McNetwork {
    pub(crate) fn new(
        rand: Pcg64,
        corrupt_rate: f64,
        dupl_rate: f64,
        drop_rate: f64,
        drop_incoming: HashSet<String>,
        drop_outgoing: HashSet<String>,
        disabled_links: HashSet<(String, String)>,
        proc_locations: HashMap<String, String>,
        events: Rc<RefCell<Vec<McEvent>>>,
    ) -> Self {
        Self {
            rand,
            corrupt_rate,
            dupl_rate,
            drop_rate,
            drop_incoming,
            drop_outgoing,
            disabled_links,
            proc_locations,
            events,
        }
    }

    pub fn get_proc_node(&self, proc: &String) -> &String {
        &self.proc_locations[proc]
    }

    pub fn send_message(&mut self, msg: Message, src: String, dest: String) {
        let src_node = self.get_proc_node(&src).clone();
        let dest_node = self.get_proc_node(&dest).clone();
        if src_node != dest_node && self.check_if_dropped(&src_node, &dest_node) {
            return;
        }
        let msg = self.corrupt_if_needed(msg);
        let data = McEvent::MessageReceived {
            msg,
            src: src.clone(),
            dest: dest.clone(),
        };
        let dups = self.duplicate_if_needed();
        for _ in 0..dups {
            self.events.borrow_mut().push(data.clone());
        }
    }

    fn rand(&mut self) -> f64 {
        self.rand.gen_range(0.0..1.0)
    }

    fn check_if_dropped(&mut self, src: &String, dest: &String) -> bool {
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
            Message::new(msg.tip, corrupted_data)
        } else {
            msg
        }
    }

    fn duplicate_if_needed(&mut self) -> u32 {
        if self.rand() >= self.dupl_rate {
            1
        } else {
            (self.rand() * 2.).ceil() as u32 + 1
        }
    }
}
