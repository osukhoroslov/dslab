use std::collections::BTreeMap;

use crate::common::AllocationVerdict;
use crate::vm::VirtualMachine;

#[derive(Debug, Clone)]
pub struct HostInfo {
    pub cpu_total: u32,
    pub memory_total: u64,

    pub cpu_available: u32,
    pub memory_available: u64,

    pub cpu_overcommit: u32,
    pub memory_overcommit: u64,

    pub vms: BTreeMap<String, VirtualMachine>,
}

impl HostInfo {
    pub fn new(cpu_total: u32, memory_total: u64, cpu_available: u32, memory_available: u64) -> Self {
        Self {
            cpu_total,
            memory_total,
            cpu_available,
            memory_available,
            cpu_overcommit: 0,
            memory_overcommit: 0,
            vms: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ResourcePoolState {
    hosts: BTreeMap<String, HostInfo>,
}

impl ResourcePoolState {
    pub fn new() -> Self {
        Self { hosts: BTreeMap::new() }
    }

    pub fn add_host(&mut self, id: &str, cpu_total: u32, memory_total: u64, cpu_available: u32, memory_available: u64) {
        self.hosts.insert(
            id.to_string(),
            HostInfo::new(cpu_total, memory_total, cpu_available, memory_available),
        );
    }

    pub fn get_hosts_list(&self) -> Vec<String> {
        self.hosts.keys().cloned().collect()
    }

    pub fn can_allocate(&self, vm: &VirtualMachine, host_id: &String) -> AllocationVerdict {
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

    pub fn get_available_cpu(&self, host_id: &String) -> u32 {
        return self.hosts[host_id].cpu_available;
    }

    pub fn get_available_memory(&self, host_id: &String) -> u64 {
        return self.hosts[host_id].memory_available;
    }

    pub fn get_cpu_load(&self, host_id: &String) -> f64 {
        return 1. - f64::from(self.hosts[host_id].cpu_available) / f64::from(self.hosts[host_id].cpu_total);
    }

    pub fn get_memory_load(&self, host_id: &String) -> f64 {
        return 1. - self.hosts[host_id].memory_available as f64 / self.hosts[host_id].memory_total as f64;
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
