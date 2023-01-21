use std::collections::{HashMap, HashSet};

use lazy_static::lazy_static;
use rand::prelude::*;
use rand_pcg::Pcg64;
use regex::Regex;

use dslab_core::Id;

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
}