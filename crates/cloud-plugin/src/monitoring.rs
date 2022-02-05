use log::info;
use std::collections::btree_map::Keys;
use std::collections::BTreeMap;

use core::actor::{Actor, ActorContext, ActorId, Event};
use core::cast;

use crate::events::monitoring::HostStateUpdate;

#[derive(Debug, Clone)]
pub struct HostState {
    pub cpu_available: u32,
    pub memory_available: u64,
    pub cpu_total: u32,
    pub memory_total: u64,
}

#[derive(Debug)]
pub struct Monitoring {
    host_states: BTreeMap<String, HostState>,
}

impl HostState {
    pub fn new(cpu_total: u32, memory_total: u64) -> Self {
        Self {
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
        }
    }

    pub fn get_host_state(&self, host: ActorId) -> HostState {
        self.host_states[&host.to_string()].clone()
    }

    pub fn get_hosts_list(&self) -> Keys<String, HostState> {
        self.host_states.keys()
    }

    pub fn add_host(&mut self, host_id: String, cpu_total: u32, memory_total: u64) {
        self.host_states
            .insert(host_id.clone(), HostState::new(cpu_total, memory_total));
    }

    fn update_host_state(
        &mut self,
        host_id: &String,
        cpu_available: u32,
        memory_available: u64,
        ctx: &mut ActorContext,
    ) {
        info!(
            "[time = {}] monitoring received stats from host #{}",
            ctx.time(),
            host_id
        );
        self.host_states.get_mut(host_id).map(|host| {
            host.cpu_available = cpu_available;
            host.memory_available = memory_available;
        });
    }
}

impl Actor for Monitoring {
    fn on(&mut self, event: Box<dyn Event>, _from: ActorId, ctx: &mut ActorContext) {
        cast!(match event {
            HostStateUpdate {
                host_id,
                cpu_available,
                memory_available,
            } => {
                self.update_host_state(host_id, *cpu_available, *memory_available, ctx);
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}
