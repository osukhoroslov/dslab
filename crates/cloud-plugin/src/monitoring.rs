use std::collections::btree_map::Keys;
use std::collections::BTreeMap;

use log::info;

use core::cast;
use core::event::Event;
use core::handler::EventHandler;

use crate::events::monitoring::HostStateUpdate;

#[derive(Debug)]
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

    pub fn get_host_state(&self, host: &str) -> &HostState {
        &self.host_states[host]
    }

    pub fn get_hosts_list(&self) -> Keys<String, HostState> {
        self.host_states.keys()
    }

    pub fn add_host(&mut self, host_id: &str, cpu_total: u32, memory_total: u64) {
        self.host_states
            .insert(host_id.to_string(), HostState::new(cpu_total, memory_total));
    }

    fn update_host_state(&mut self, host_id: String, cpu_available: u32, memory_available: u64, time: f64) {
        info!("[time = {}] monitoring received stats from host #{}", time, host_id);
        self.host_states.get_mut(&host_id).map(|host| {
            host.cpu_available = cpu_available;
            host.memory_available = memory_available;
        });
    }
}

impl EventHandler for Monitoring {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            HostStateUpdate {
                host_id,
                cpu_available,
                memory_available,
            } => {
                self.update_host_state(host_id, cpu_available, memory_available, event.time.into_inner());
            }
        })
    }
}
