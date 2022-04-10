use std::collections::btree_map::Keys;
use std::collections::BTreeMap;
use std::collections::HashMap;

use simcore::cast;
use simcore::context::SimulationContext;
use simcore::event::Event;
use simcore::handler::EventHandler;
use simcore::log_debug;

use crate::events::monitoring::HostStateUpdate;

#[derive(Debug)]
pub struct HostState {
    pub cpu_load: f64,
    pub memory_load: f64,
    pub cpu_total: u32,
    pub memory_total: u64,
}

pub struct Monitoring {
    host_states: BTreeMap<u32, HostState>,
    vm_locations: HashMap<u32, u32>,
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
            vm_locations: HashMap::new(),
            ctx,
        }
    }

    pub fn get_host_state(&self, host: u32) -> &HostState {
        &self.host_states[&host]
    }

    pub fn get_hosts_list(&self) -> Keys<u32, HostState> {
        self.host_states.keys()
    }

    pub fn add_host(&mut self, host_id: u32, cpu_total: u32, memory_total: u64) {
        self.host_states
            .insert(host_id, HostState::new(cpu_total, memory_total));
    }

    pub fn find_host_by_vm(&mut self, vm_id: u32) -> u32 {
        return *self.vm_locations.get(&vm_id).unwrap();
    }

    fn update_host_state(
        &mut self,
        host_id: u32,
        cpu_load: f64,
        memory_load: f64,
        previously_added_vms: Vec<u32>,
        previously_removed_vms: Vec<u32>,
    ) {
        log_debug!(self.ctx, "monitoring received stats from host #{}", host_id);
        self.host_states.get_mut(&host_id).map(|host| {
            host.cpu_load = cpu_load;
            host.memory_load = memory_load;
        });

        for vm_id in previously_added_vms {
            self.vm_locations.insert(vm_id, host_id);
        }
        for vm_id in previously_removed_vms {
            if self.vm_locations.contains_key(&vm_id) {
                self.vm_locations.remove(&vm_id);
            }
        }
    }
}

impl EventHandler for Monitoring {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            HostStateUpdate {
                host_id,
                cpu_load,
                memory_load,
                previously_added_vms,
                previously_removed_vms,
            } => {
                self.update_host_state(
                    host_id,
                    cpu_load,
                    memory_load,
                    previously_added_vms,
                    previously_removed_vms,
                );
            }
        })
    }
}
