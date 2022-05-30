use std::collections::btree_map::Keys;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;

use simcore::cast;
use simcore::context::SimulationContext;
use simcore::event::Event;
use simcore::handler::EventHandler;
use simcore::log_trace;

use crate::core::common::Allocation;
use crate::core::events::monitoring::HostStateUpdate;
use crate::core::vm::{VirtualMachine, VmStatus};

#[derive(Clone)]
pub struct HostState {
    pub cpu_load: f64,
    pub memory_load: f64,
    pub cpu_total: u32,
    pub memory_total: u64,
    pub allocations: BTreeMap<u32, Allocation>,
    pub vms: BTreeMap<u32, VirtualMachine>,
}

pub struct Monitoring {
    host_states: BTreeMap<u32, HostState>,
    vm_locations: HashMap<u32, u32>,
    host_vms: BTreeMap<u32, BTreeSet<u32>>,
    ctx: SimulationContext,
}

impl HostState {
    pub fn new(cpu_total: u32, memory_total: u64) -> Self {
        Self {
            cpu_load: 0.,
            memory_load: 0.,
            cpu_total,
            memory_total,
            allocations: BTreeMap::new(),
            vms: BTreeMap::new(),
        }
    }
}

impl Monitoring {
    pub fn new(ctx: SimulationContext) -> Self {
        Self {
            host_states: BTreeMap::new(),
            vm_locations: HashMap::new(),
            host_vms: BTreeMap::new(),
            ctx,
        }
    }

    pub fn get_host_state(&self, host: u32) -> &HostState {
        &self.host_states[&host]
    }

    pub fn get_hosts_list(&self) -> Keys<u32, HostState> {
        self.host_states.keys()
    }

    pub fn get_host_vms(&self, host: u32) -> BTreeSet<u32> {
        self.host_vms[&host].clone()
    }

    pub fn get_allocation(&self, host_id: u32, alloc_id: u32) -> Allocation {
        self.host_states
            .get(&host_id)
            .unwrap()
            .allocations
            .get(&alloc_id)
            .unwrap()
            .clone()
    }

    pub fn get_vm(&self, host_id: u32, vm_id: u32) -> VirtualMachine {
        self.host_states.get(&host_id).unwrap().vms.get(&vm_id).unwrap().clone()
    }

    pub fn get_host_states(&self) -> &BTreeMap<u32, HostState> {
        &self.host_states
    }

    pub fn add_host(&mut self, host_id: u32, cpu_total: u32, memory_total: u64) {
        self.host_states
            .insert(host_id, HostState::new(cpu_total, memory_total));
        self.host_vms.insert(host_id, BTreeSet::<u32>::new());
    }

    pub fn find_host_by_vm(&self, vm_id: u32) -> u32 {
        return *self.vm_locations.get(&vm_id).unwrap();
    }

    fn update_host_state(
        &mut self,
        host_id: u32,
        cpu_load: f64,
        memory_load: f64,
        recently_added_vms: Vec<(Allocation, VirtualMachine)>,
        recently_removed_vms: Vec<u32>,
        recent_vm_status_changes: HashMap<u32, (VmStatus, f64)>,
    ) {
        log_trace!(self.ctx, "monitoring received stats from host #{}", host_id);
        self.host_states.get_mut(&host_id).map(|host| {
            host.cpu_load = cpu_load;
            host.memory_load = memory_load;

            for (alloc, vm) in recently_added_vms {
                self.vm_locations.insert(alloc.id, host_id);
                self.host_vms.get_mut(&host_id).unwrap().insert(alloc.id);
                host.vms.insert(alloc.id, vm);
                host.allocations.insert(alloc.id, alloc);
            }

            for (vm_id, (status, time)) in recent_vm_status_changes {
                let vm = host.vms.get_mut(&vm_id).unwrap();
                if status == VmStatus::Running {
                    // update start time, so that it can be passed during VM migration
                    vm.set_start_time(time);
                }
                vm.set_status(status);
            }

            for vm_id in recently_removed_vms {
                if self.vm_locations.get(&vm_id) == Some(&host_id) {
                    self.vm_locations.remove(&vm_id);
                }
                self.host_vms.get_mut(&host_id).unwrap().remove(&vm_id);
                host.allocations.remove(&vm_id);
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
                recent_vm_status_changes,
            } => {
                self.update_host_state(
                    host_id,
                    cpu_load,
                    memory_load,
                    recently_added_vms,
                    recently_removed_vms,
                    recent_vm_status_changes,
                );
            }
        })
    }
}
