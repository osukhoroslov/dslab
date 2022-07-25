use std::collections::btree_map::Keys;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

use dslab_core::cast;
use dslab_core::context::SimulationContext;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;
use dslab_core::log_trace;

use crate::core::events::monitoring::HostStateUpdate;

#[derive(Clone)]
pub struct HostState {
    pub cpu_load: f64,
    pub memory_load: f64,
    pub cpu_total: u32,
    pub memory_total: u64,
    pub vms: BTreeSet<u32>,
}

pub struct Monitoring {
    host_states: BTreeMap<u32, HostState>,
    ctx: SimulationContext,
}

impl HostState {
    pub fn new(cpu_total: u32, memory_total: u64) -> Self {
        Self {
            cpu_load: 0.,
            memory_load: 0.,
            cpu_total,
            memory_total,
            vms: BTreeSet::new(),
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

    pub fn get_id(&self) -> u32 {
        self.ctx.id()
    }

    pub fn get_host_state(&self, host: u32) -> &HostState {
        &self.host_states[&host]
    }

    pub fn get_hosts_list(&self) -> Keys<u32, HostState> {
        self.host_states.keys()
    }

    pub fn get_host_vms(&self, host: u32) -> BTreeSet<u32> {
        self.host_states[&host].vms.clone()
    }

    pub fn get_host_states(&self) -> &BTreeMap<u32, HostState> {
        &self.host_states
    }

    pub fn add_host(&mut self, host_id: u32, cpu_total: u32, memory_total: u64) {
        self.host_states
            .insert(host_id, HostState::new(cpu_total, memory_total));
    }

    fn update_host_state(
        &mut self,
        host_id: u32,
        cpu_load: f64,
        memory_load: f64,
        recently_added_vms: Vec<u32>,
        recently_removed_vms: Vec<u32>,
    ) {
        log_trace!(self.ctx, "monitoring received stats from host #{}", host_id);
        self.host_states.get_mut(&host_id).map(|host| {
            host.cpu_load = cpu_load;
            host.memory_load = memory_load;

            for vm_id in recently_added_vms {
                host.vms.insert(vm_id);
            }

            for vm_id in recently_removed_vms {
                host.vms.remove(&vm_id);
            }
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
                recently_added_vms,
                recently_removed_vms,
            } => {
                self.update_host_state(host_id, cpu_load, memory_load, recently_added_vms, recently_removed_vms);
            }
        })
    }
}
