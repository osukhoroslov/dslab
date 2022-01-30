use std::collections::BTreeMap;

use crate::host::AllocationVerdict;
use crate::monitoring::HostState;
use crate::virtual_machine::VirtualMachine;

#[derive(Debug, Clone)]
pub struct HostInfo {
    pub cpu_available: u64,
    pub memory_available: u64,

    pub cpu_total: u64,
    pub memory_total: u64,

    pub cpu_overcommit: u64,
    pub memory_overcommit: u64,

    pub vms: BTreeMap<String, VirtualMachine>,
}

impl HostInfo {
    pub fn new(cpu_available: u64, memory_available: u64, cpu_total: u64, memory_total: u64) -> Self {
        Self {
            cpu_available: cpu_available,
            memory_available: memory_available,
            cpu_total,
            memory_total,
            cpu_overcommit: 0,
            memory_overcommit: 0,
            vms: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Store {
    hosts: BTreeMap<String, HostInfo>,
}

impl Store {
    pub fn new() -> Self {
        Self { hosts: BTreeMap::new() }
    }

    pub fn add_host(&mut self, id: String, state: &HostState) {
        self.hosts.insert(
            id,
            HostInfo::new(
                state.cpu_available,
                state.memory_available,
                state.cpu_total,
                state.memory_total,
            ),
        );
    }

    pub fn get_hosts_list(&self) -> Vec<String> {
        self.hosts.keys().cloned().collect()
    }

    pub fn can_allocate(&mut self, vm: &VirtualMachine, host_id: &String) -> AllocationVerdict {
        if !self.hosts.contains_key(host_id) {
            return AllocationVerdict::HostNotFound;
        }
        if self.hosts[host_id].cpu_available < vm.cpu_usage {
            return AllocationVerdict::NotEnoughCPU;
        }
        if self.hosts[host_id].memory_available < vm.memory_usage {
            return AllocationVerdict::NotEnoughMemory;
        }
        return AllocationVerdict::Success;
    }

    pub fn place_vm(&mut self, vm: &VirtualMachine, host_id: &String) {
        self.hosts.get_mut(host_id).map(|host| {
            if host.vms.contains_key(&vm.id) {
                return;
            }

            if host.cpu_available < vm.cpu_usage {
                host.cpu_overcommit += vm.cpu_usage - host.cpu_available;
                host.cpu_available = 0;
            } else {
                host.cpu_available -= vm.cpu_usage;
            }

            if host.memory_available < vm.memory_usage {
                host.memory_overcommit += vm.memory_usage - host.memory_available;
                host.memory_available = 0;
            } else {
                host.memory_available -= vm.memory_usage;
            }

            host.vms.insert(vm.id.clone(), vm.clone());
        });
    }

    pub fn remove_vm(&mut self, vm: &VirtualMachine, host_id: &String) {
        self.hosts.get_mut(host_id).map(|host| {
            if host.cpu_overcommit >= vm.cpu_usage {
                host.cpu_overcommit -= vm.cpu_usage;
            } else {
                host.cpu_available += vm.cpu_usage - host.cpu_overcommit;
                host.cpu_overcommit = 0;
            }

            if host.memory_overcommit >= vm.memory_usage {
                host.memory_overcommit -= vm.memory_usage;
            } else {
                host.memory_available += vm.memory_usage - host.memory_overcommit;
                host.memory_overcommit = 0;
            }

            host.vms.remove(&vm.id);
        });
    }
}
