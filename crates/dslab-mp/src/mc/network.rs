use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use lazy_static::lazy_static;
use rand::prelude::*;
use rand_pcg::Pcg64;
use regex::Regex;

use dslab_core::{Event, Id};

use crate::events::{MessageReceived, TimerFired};
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
    node_ids: HashMap<String, Id>,
    events: Rc<RefCell<Vec<Event>>>,
    event_count: u64,
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
        node_ids: HashMap<String, Id>,
        events: Rc<RefCell<Vec<Event>>>,
        event_count: u64,
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
            node_ids,
            events,
            event_count,
        }
    }

    pub fn rand(&mut self) -> f64 {
        self.rand.gen_range(0.0..1.0)
    }

    pub fn corrupt_if_needed(&mut self, msg: Message) -> Message {
        if self.rand() < self.corrupt_rate {
            lazy_static! {
                static ref RE: Regex = Regex::new(r#""\w+""#).unwrap();
            }
            let corrupted_data = RE.replace_all(&msg.data, "\"\"").to_string();
            let corrupted_msg = Message::new(msg.tip, corrupted_data);
            corrupted_msg
        } else {
            msg
        }
    }

    pub fn dest_node_id(&self, dest: &str) -> Id {
        let dest_node = self.proc_locations.get(dest).unwrap();
        *self.node_ids.get(dest_node).unwrap()
    }

    pub fn proc_locations(&self) -> &HashMap<String, String> {
        &self.proc_locations
    }

    pub fn duplicate_if_needed(&mut self) -> u32 {
        if self.rand() >= self.dupl_rate {
            1
        } else {
            (self.rand() * 2.).ceil() as u32 + 1
        }
    }

    pub fn check_if_dropped(&mut self, src: &String, dest: &String) -> bool {
        self.rand() < self.drop_rate
            || self.drop_outgoing.contains(src)
            || self.drop_incoming.contains(dest)
            || self.disabled_links.contains(&(src.clone(), dest.clone()))
    }

    pub fn send_message(&mut self, msg: Message, src: String, dest: String, src_id: Id) -> bool {
        let msg = self.corrupt_if_needed(msg);
        let data = MessageReceived {
            msg,
            src: src.clone(),
            dest: dest.clone(),
        };
        let event = Event {
            id: self.event_count,
            time: 0.0,
            src: src_id,
            dest: self.dest_node_id(&dest),
            data: Box::new(data),
        };
        let proc_locations = self.proc_locations();
        let src_node = proc_locations[&src].clone();
        let dest_node = proc_locations[&dest].clone();
        if event.src != event.dest && self.check_if_dropped(&src_node, &dest_node) {
            return false;
        }
        let dups = self.duplicate_if_needed();
        for _i in 0..dups {
            self.events.borrow_mut().push(event.clone());
        }
        self.event_count += 1;
        return true;
    }

    pub fn set_timer(&mut self, name: String, proc: String, src_id: Id) -> u64 {
        let data = TimerFired {
            timer: name.clone(),
            proc: proc.clone(),
        };
        let event = Event {
            id: self.event_count,
            time: 0.0,
            src: src_id,
            dest: src_id,
            data: Box::new(data),
        };
        self.events.borrow_mut().push(event);
        self.event_count += 1;
        self.event_count
    }
}
