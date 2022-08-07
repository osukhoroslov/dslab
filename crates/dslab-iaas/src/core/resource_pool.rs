//! Local copy of cluster state database.

use std::collections::BTreeMap;

use crate::core::common::{Allocation, AllocationVerdict};

/// Host state, allocated resources and set of VM on that host.
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
    /// Initialize structure.
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
    /// Create storage.
    pub fn new() -> Self {
        Self { hosts: BTreeMap::new() }
    }

    /// Add host to cluster.
    pub fn add_host(&mut self, id: u32, cpu_total: u32, memory_total: u64, cpu_available: u32, memory_available: u64) {
        self.hosts.insert(
            id,
            HostInfo::new(cpu_total, memory_total, cpu_available, memory_available),
        );
    }

    /// Get all cluster hosts.
    pub fn get_hosts_list(&self) -> Vec<u32> {
        self.hosts.keys().cloned().collect()
    }

    /// Check if allocation of such VM is possible on specified host.
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

    /// Allocate VM on specified host.
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

    /// Release resources due to VM lifecycle finish.
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

    /// Get remaining host CPU.
    pub fn get_available_cpu(&self, host_id: u32) -> u32 {
        return self.hosts[&host_id].cpu_available;
    }

    /// Get remaining host RAM.
    pub fn get_available_memory(&self, host_id: u32) -> u64 {
        return self.hosts[&host_id].memory_available;
    }

    /// Get host CPU load (ratio pf allocated resources, not actual load).
    pub fn get_cpu_load(&self, host_id: u32) -> f64 {
        return 1. - self.hosts[&host_id].cpu_available as f64 / self.hosts[&host_id].cpu_total as f64;
    }

    /// Get host RAM load (ratio pf allocated resources, not actual load).
    pub fn get_memory_load(&self, host_id: u32) -> f64 {
        return 1. - self.hosts[&host_id].memory_available as f64 / self.hosts[&host_id].memory_total as f64;
    }
}
