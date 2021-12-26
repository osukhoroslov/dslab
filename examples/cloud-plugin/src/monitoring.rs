use log::info;
use std::collections::btree_map::Keys;
use std::collections::BTreeMap;

use core::actor::{Actor, ActorContext, ActorId, Event};
use core::cast;

// ACTORS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct HostState {
    pub id: ActorId,
    pub cpu_available: u32,
    pub ram_available: u32,
}

#[derive(Debug)]
pub struct Monitoring {
    host_states: BTreeMap<String, HostState>,
}

impl HostState {
    pub fn new(id: ActorId) -> Self {
        Self {
            id,
            cpu_available: 0,
            ram_available: 0,
        }
    }
}

impl Monitoring {
    pub fn new() -> Self {
        Self {
            host_states: BTreeMap::new(),
        }
    }

    pub fn add_host(&mut self, host: ActorId) {
        self.host_states.entry(host.to_string()).or_insert(HostState::new(host));
    }

    pub fn get_host_state(&self, host: ActorId) -> HostState {
        self.host_states[&host.to_string()].clone()
    }

    pub fn get_hosts_list(&self) -> Keys<String, HostState> {
        self.host_states.keys()
    }
}

// EVENTS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct HostStateUpdate {
    pub host_id: ActorId,
    pub cpu_available: u32,
    pub ram_available: u32,
}

impl Actor for Monitoring {
    fn on(&mut self, event: Box<dyn Event>, _from: ActorId, ctx: &mut ActorContext) {
        cast!(match event {
            HostStateUpdate {
                host_id,
                cpu_available,
                ram_available,
            } => {
                info!(
                    "[time = {}] monitoring received stats from host #{}",
                    ctx.time(),
                    host_id
                );
                if !self.host_states.contains_key(&host_id.to_string()) {
                    self.add_host(host_id.clone());
                }

                self.host_states.get_mut(&host_id.to_string()).unwrap().cpu_available = *cpu_available;
                self.host_states.get_mut(&host_id.to_string()).unwrap().ram_available = *ram_available;
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}
