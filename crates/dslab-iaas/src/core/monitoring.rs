//! Physical machines actual load.

use std::collections::btree_map::Keys;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

use dslab_core::cast;
use dslab_core::context::SimulationContext;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;
use dslab_core::log_trace;

use crate::core::events::monitoring::HostStateUpdate;

/// Host state contains resource capacity and current actual load. In addition a set of active VMs is stored.
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
    /// Create component.
    pub fn new(ctx: SimulationContext) -> Self {
        Self {
            host_states: BTreeMap::new(),
            ctx,
        }
    }

    /// Get component ID.
    pub fn get_id(&self) -> u32 {
        self.ctx.id()
    }

    /// Get host state reference.
    pub fn get_host_state(&self, host: u32) -> &HostState {
        &self.host_states[&host]
    }

    /// Get all cluster hosts iterator.
    pub fn get_hosts_list(&self) -> Keys<u32, HostState> {
        self.host_states.keys()
    }

    /// Get all active VMS on specified host.
    pub fn get_host_vms(&self, host: u32) -> BTreeSet<u32> {
        self.host_states[&host].vms.clone()
    }

    /// Get all host states.
    pub fn get_host_states(&self) -> &BTreeMap<u32, HostState> {
        &self.host_states
    }

    /// Add new host to central database.
    pub fn add_host(&mut self, host_id: u32, cpu_total: u32, memory_total: u64) {
        self.host_states
            .insert(host_id, HostState::new(cpu_total, memory_total));
    }

    /// Hosts periodically send their states to main storage.
    /// This function processes these reports.
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
