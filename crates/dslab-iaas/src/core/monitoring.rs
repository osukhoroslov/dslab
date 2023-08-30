//! Service that provides information about current state of hosts.

use std::cell::RefCell;
use std::rc::Rc;

use std::collections::btree_map::Keys;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

use dslab_core::cast;
use dslab_core::context::SimulationContext;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;

use crate::core::events::monitoring::HostStateUpdate;
use crate::core::logger::Logger;

/// Host state contains resource capacity and current actual load. In addition a set of active VMs is stored.
#[derive(Clone)]
pub struct HostState {
    pub cpu_load: f64,
    pub memory_load: f64,
    pub cpu_total: u32,
    pub memory_total: u64,
    pub vms: BTreeSet<u32>,
}

/// This component stores the information about current host states received from host managers and provides this
/// information to other components such as scheduler. Just like in a real system, the information arrives to the
/// monitoring with some delay, so it can be outdated.
pub struct Monitoring {
    host_states: BTreeMap<u32, HostState>,
    ctx: SimulationContext,
    logger: Rc<RefCell<Box<dyn Logger>>>,
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
    /// Creates component.
    pub fn new(ctx: SimulationContext, logger: Rc<RefCell<Box<dyn Logger>>>) -> Self {
        Self {
            host_states: BTreeMap::new(),
            ctx,
            logger,
        }
    }

    /// Returns component ID.
    pub fn get_id(&self) -> u32 {
        self.ctx.id()
    }

    /// Returns the state of specified host.
    pub fn get_host_state(&self, host: u32) -> &HostState {
        &self.host_states[&host]
    }

    /// Returns an iterator of IDs and states of all hosts.
    pub fn get_hosts_list(&self) -> Keys<u32, HostState> {
        self.host_states.keys()
    }

    /// Returns IDs of active VMS on the specified host.
    pub fn get_host_vms(&self, host: u32) -> BTreeSet<u32> {
        self.host_states[&host].vms.clone()
    }

    /// Get all host states.
    pub fn get_host_states(&self) -> &BTreeMap<u32, HostState> {
        &self.host_states
    }

    /// Adds new host to internal storage.
    pub fn add_host(&mut self, host_id: u32, cpu_total: u32, memory_total: u64) {
        self.host_states
            .insert(host_id, HostState::new(cpu_total, memory_total));
    }

    /// Processes periodic host state updates received from host manages.
    fn update_host_state(
        &mut self,
        host_id: u32,
        cpu_load: f64,
        memory_load: f64,
        recently_added_vms: Vec<u32>,
        recently_removed_vms: Vec<u32>,
    ) {
        self.logger
            .borrow_mut()
            .log_trace(&self.ctx, format!("monitoring received stats from host #{}", host_id));
        if let Some(host) = self.host_states.get_mut(&host_id) {
            host.cpu_load = cpu_load;
            host.memory_load = memory_load;

            for vm_id in recently_added_vms {
                host.vms.insert(vm_id);
            }

            for vm_id in recently_removed_vms {
                host.vms.remove(&vm_id);
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
                recently_added_vms,
                recently_removed_vms,
            } => {
                self.update_host_state(host_id, cpu_load, memory_load, recently_added_vms, recently_removed_vms);
            }
        })
    }
}
