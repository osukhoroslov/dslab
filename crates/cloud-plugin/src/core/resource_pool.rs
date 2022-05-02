use std::collections::BTreeMap;

use serde::Serialize;

use crate::core::common::AllocationVerdict;

#[derive(Serialize, Clone)]
pub struct Allocation {
    pub id: u32,
    pub cpu_usage: u32,
    pub memory_usage: u64,
}

#[derive(Clone)]
pub struct HostInfo {
    pub cpu_total: u32,
    pub memory_total: u64,

    pub cpu_available: u32,
    pub memory_available: u64,

    pub cpu_overcommit: u32,
    pub memory_overcommit: u64,

    pub allocations: BTreeMap<u32, Allocation>,
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
            allocations: BTreeMap::new(),
        }
    }
}

#[derive(Clone)]
pub struct ResourcePoolState {
    hosts: BTreeMap<u32, HostInfo>,
}

impl ResourcePoolState {
    pub fn new() -> Self {
        Self { hosts: BTreeMap::new() }
    }

    pub fn add_host(&mut self, id: u32, cpu_total: u32, memory_total: u64, cpu_available: u32, memory_available: u64) {
        self.hosts.insert(
            id,
            HostInfo::new(cpu_total, memory_total, cpu_available, memory_available),
        );
    }

    pub fn get_hosts_list(&self) -> Vec<u32> {
        self.hosts.keys().cloned().collect()
    }

    pub fn can_allocate(&self, alloc: &Allocation, host_id: u32) -> AllocationVerdict {
        if !self.hosts.contains_key(&host_id) {
            return AllocationVerdict::HostNotFound;
        }
        if self.hosts[&host_id].cpu_available < alloc.cpu_usage {
            return AllocationVerdict::NotEnoughCPU;
        }
        if self.hosts[&host_id].memory_available < alloc.memory_usage {
            return AllocationVerdict::NotEnoughMemory;
        }
        return AllocationVerdict::Success;
    }

    pub fn allocate(&mut self, alloc: &Allocation, host_id: u32) {
        self.hosts.get_mut(&host_id).map(|host| {
            if host.allocations.contains_key(&alloc.id) {
                return;
            }

            if host.cpu_available < alloc.cpu_usage {
                host.cpu_overcommit += alloc.cpu_usage - host.cpu_available;
                host.cpu_available = 0;
            } else {
                host.cpu_available -= alloc.cpu_usage;
            }

            if host.memory_available < alloc.memory_usage {
                host.memory_overcommit += alloc.memory_usage - host.memory_available;
                host.memory_available = 0;
            } else {
                host.memory_available -= alloc.memory_usage;
            }

            host.allocations.insert(alloc.id, alloc.clone());
        });
    }

    pub fn release(&mut self, alloc: &Allocation, host_id: u32) {
        self.hosts.get_mut(&host_id).map(|host| {
            if host.cpu_overcommit >= alloc.cpu_usage {
                host.cpu_overcommit -= alloc.cpu_usage;
            } else {
                host.cpu_available += alloc.cpu_usage - host.cpu_overcommit;
                host.cpu_overcommit = 0;
            }

            if host.memory_overcommit >= alloc.memory_usage {
                host.memory_overcommit -= alloc.memory_usage;
            } else {
                host.memory_available += alloc.memory_usage - host.memory_overcommit;
                host.memory_overcommit = 0;
            }

            host.allocations.remove(&alloc.id);
        });
    }

    pub fn get_available_cpu(&self, host_id: u32) -> u32 {
        return self.hosts[&host_id].cpu_available;
    }

    pub fn get_available_memory(&self, host_id: u32) -> u64 {
        return self.hosts[&host_id].memory_available;
    }

    pub fn get_cpu_load(&self, host_id: u32) -> f64 {
        return 1. - self.hosts[&host_id].cpu_available as f64 / self.hosts[&host_id].cpu_total as f64;
    }

    pub fn get_memory_load(&self, host_id: u32) -> f64 {
        return 1. - self.hosts[&host_id].memory_available as f64 / self.hosts[&host_id].memory_total as f64;
    }
}
