//! Resource pool state.

use std::collections::BTreeMap;

use crate::core::common::{Allocation, AllocationVerdict};

/// Stores host properties (resource capacity) and state (available resources, current allocations).
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
    /// Creates host info with specified total and available host capacity.
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
    /// Creates empty resource pool state.
    pub fn new() -> Self {
        Self { hosts: BTreeMap::new() }
    }

    /// Adds host to resource pool.
    pub fn add_host(&mut self, id: u32, cpu_total: u32, memory_total: u64, cpu_available: u32, memory_available: u64) {
        self.hosts.insert(
            id,
            HostInfo::new(cpu_total, memory_total, cpu_available, memory_available),
        );
    }

    /// Returns IDs of all hosts.
    pub fn get_hosts_list(&self) -> Vec<u32> {
        self.hosts.keys().cloned().collect()
    }

    /// Returns the number of hosts.
    pub fn get_host_count(&self) -> u32 {
        self.hosts.len() as u32
    }

    /// Checks if the specified allocation is currently possible on the specified host.
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

    /// Applies the specified application on the specified host.
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

    /// Removes the specified allocation on the specified host.
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

    /// Returns the total CPU capacity of the specified host.
    pub fn get_total_cpu(&self, host_id: u32) -> u32 {
        self.hosts[&host_id].cpu_total
    }

    /// Returns the total memory capacity of the specified host.
    pub fn get_total_memory(&self, host_id: u32) -> u64 {
        self.hosts[&host_id].memory_total
    }

    /// Returns the amount of available vCPUs on the specified host.
    pub fn get_available_cpu(&self, host_id: u32) -> u32 {
        self.hosts[&host_id].cpu_available
    }

    /// Returns the amount of available memory on the specified host.
    pub fn get_available_memory(&self, host_id: u32) -> u64 {
        self.hosts[&host_id].memory_available
    }

    /// Returns CPU capacity of the specified host currently in use by some VMs.
    pub fn get_allocated_cpu(&self, host_id: u32) -> u32 {
        self.get_total_cpu(host_id) - self.get_available_cpu(host_id)
    }

    /// Returns memory capacity of the specified host currently in use by some VMs.
    pub fn get_allocated_memory(&self, host_id: u32) -> u64 {
        self.get_total_memory(host_id) - self.get_available_memory(host_id)
    }

    /// Returns the CPU allocation rate (ratio of allocated to total resources) of the specified host
    pub fn get_cpu_load(&self, host_id: u32) -> f64 {
        1. - self.hosts[&host_id].cpu_available as f64 / self.hosts[&host_id].cpu_total as f64
    }

    /// Returns the memory allocation rate (ratio of allocated to total resources) of the specified host
    pub fn get_memory_load(&self, host_id: u32) -> f64 {
        1. - self.hosts[&host_id].memory_available as f64 / self.hosts[&host_id].memory_total as f64
    }
}
