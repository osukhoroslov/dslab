use log::info;
use std::collections::btree_map::Keys;
use std::collections::BTreeMap;
use std::collections::HashSet;

use core::actor::{Actor, ActorContext, ActorId, Event};
use core::cast;

// ACTORS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct HostState {
    pub id: ActorId,
    pub cpu_available: u32,
    pub memory_available: u32,
    pub cpu_total: u32,
    pub memory_total: u32,
}

#[derive(Debug)]
pub struct Monitoring {
    host_states: BTreeMap<String, HostState>,
    schedulers: HashSet<String>,
}

impl HostState {
    pub fn new(id: ActorId, cpu_total: u32, memory_total: u32) -> Self {
        Self {
            id,
            cpu_available: cpu_total,
            memory_available: memory_total,
            cpu_total,
            memory_total,
        }
    }
}

impl Monitoring {
    pub fn new() -> Self {
        Self {
            host_states: BTreeMap::new(),
            schedulers: HashSet::new(),
        }
    }

    pub fn get_host_state(&self, host: ActorId) -> HostState {
        self.host_states[&host.to_string()].clone()
    }

    pub fn get_hosts_list(&self) -> Keys<String, HostState> {
        self.host_states.keys()
    }

    pub fn get_schedulers_list(&self) -> Vec<String> {
        self.schedulers.clone().into_iter().collect::<Vec<String>>()
    }

    pub fn add_scheduler(&mut self, scheduler_actor_id: String) {
        self.schedulers.insert(scheduler_actor_id.clone());
    }

    pub fn add_host(&mut self, host_id: String, cpu_total: u32, memory_total: u32) {
        self.host_states.insert(
            host_id.clone(),
            HostState::new(ActorId::from(&host_id), cpu_total, memory_total),
        );
    }
}

// EVENTS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct HostStateUpdate {
    pub host_id: String,
    pub cpu_available: u32,
    pub memory_available: u32,
}

impl Actor for Monitoring {
    fn on(&mut self, event: Box<dyn Event>, _from: ActorId, ctx: &mut ActorContext) {
        cast!(match event {
            HostStateUpdate {
                host_id,
                cpu_available,
                memory_available,
            } => {
                info!(
                    "[time = {}] monitoring received stats from host #{}",
                    ctx.time(),
                    host_id
                );
                self.host_states.get_mut(host_id).map(|host| {
                    host.cpu_available = *cpu_available;
                    host.memory_available = *memory_available;
                });
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}
