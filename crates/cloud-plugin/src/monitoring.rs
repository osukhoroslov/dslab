use std::collections::btree_map::Keys;
use std::collections::BTreeMap;

use log::info;

use simcore::cast;
use simcore::event::Event;
use simcore::handler::EventHandler;

use crate::events::monitoring::HostStateUpdate;

#[derive(Debug)]
pub struct HostState {
    pub cpu_load: f64,
    pub memory_load: f64,
    pub cpu_total: u32,
    pub memory_total: u64,
}

pub struct Monitoring {
    host_states: BTreeMap<String, HostState>,
    ctx: SimulationContext,
}

impl HostState {
    pub fn new(cpu_total: u32, memory_total: u64) -> Self {
        Self {
            cpu_load: 0.,
            memory_load: 0.,
            cpu_total,
            memory_total,
        }
    }
}

impl Monitoring {
    pub fn new(ctx: SimulationContext) -> Self {
        Self {
            host_states: BTreeMap::new(),
            ctx,
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

    fn update_host_state(&mut self, host_id: String, cpu_load: f64, memory_load: f64) {
        log_debug!(self.ctx, "monitoring received stats from host #{}", host_id);
        self.host_states.get_mut(&host_id).map(|host| {
            host.cpu_load = cpu_load;
            host.memory_load = memory_load;
        });
    }
}

impl EventHandler for Monitoring {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            HostStateUpdate {
                host_id,
                cpu_load,
                memory_load,
            } => {
                self.update_host_state(host_id, cpu_load, memory_load);
            }
        })
    }
}
