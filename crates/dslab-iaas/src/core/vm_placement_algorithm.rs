//! Host selection algorithms.

use crate::core::common::Allocation;
use crate::core::common::AllocationVerdict;
use crate::core::monitoring::Monitoring;
use crate::core::resource_pool::ResourcePoolState;

/// Interface of host selection algorithm.
/// alloc - allocated VM capacity
/// pool_state - allocated resources on cluster hosts
/// monitoring - host actual loads
/// The algorithm returns ID of selected host where resources will be allocated.
pub trait VMPlacementAlgorithm {
    fn select_host(&self, alloc: &Allocation, pool_state: &ResourcePoolState, monitoring: &Monitoring) -> Option<u32>;
}

////////////////////////////////////////////////////////////////////////////////

/// First fit algorithm.
pub struct FirstFit;

impl FirstFit {
    pub fn new() -> Self {
        Self {}
    }
}

impl VMPlacementAlgorithm for FirstFit {
    fn select_host(&self, alloc: &Allocation, pool_state: &ResourcePoolState, _monitoring: &Monitoring) -> Option<u32> {
        for host in pool_state.get_hosts_list() {
            if pool_state.can_allocate(&alloc, host) == AllocationVerdict::Success {
                return Some(host);
            }
        }
        return None;
    }
}

////////////////////////////////////////////////////////////////////////////////

/// Best fit algorithm. Select the most CPU loaded machine.
pub struct BestFit;

impl BestFit {
    pub fn new() -> Self {
        Self {}
    }
}

impl VMPlacementAlgorithm for BestFit {
    fn select_host(&self, alloc: &Allocation, pool_state: &ResourcePoolState, _monitoring: &Monitoring) -> Option<u32> {
        let mut result: Option<u32> = None;
        let mut min_available_cpu: u32 = u32::MAX;

        for host in pool_state.get_hosts_list() {
            if pool_state.can_allocate(&alloc, host) == AllocationVerdict::Success {
                if pool_state.get_available_cpu(host) < min_available_cpu {
                    min_available_cpu = pool_state.get_available_cpu(host);
                    result = Some(host);
                }
            }
        }
        return result;
    }
}

////////////////////////////////////////////////////////////////////////////////

/// Best fit algorithm. Select the least CPU loaded machine.
pub struct WorstFit;

impl WorstFit {
    pub fn new() -> Self {
        Self {}
    }
}

impl VMPlacementAlgorithm for WorstFit {
    fn select_host(&self, alloc: &Allocation, pool_state: &ResourcePoolState, _monitoring: &Monitoring) -> Option<u32> {
        let mut result: Option<u32> = None;
        let mut max_available_cpu: u32 = 0;

        for host in pool_state.get_hosts_list() {
            if pool_state.can_allocate(&alloc, host) == AllocationVerdict::Success {
                if pool_state.get_available_cpu(host) > max_available_cpu {
                    max_available_cpu = pool_state.get_available_cpu(host);
                    result = Some(host);
                }
            }
        }
        return result;
    }
}

////////////////////////////////////////////////////////////////////////////////

/// Best fit algorithm. Select the most CPU loaded machine by actual load.
pub struct BestFitThreshold {
    threshold: f64,
}

impl BestFitThreshold {
    pub fn new(threshold: f64) -> Self {
        Self { threshold }
    }
}

impl VMPlacementAlgorithm for BestFitThreshold {
    fn select_host(&self, alloc: &Allocation, _pool_state: &ResourcePoolState, monitoring: &Monitoring) -> Option<u32> {
        let mut result: Option<u32> = None;
        let mut best_cpu_load: f64 = 0.;
        for host in monitoring.get_hosts_list() {
            let state = monitoring.get_host_state(*host);
            let cpu_used = state.cpu_load * state.cpu_total as f64;
            let memory_used = state.memory_load * state.memory_total as f64;

            let cpu_load_new = (cpu_used + alloc.cpu_usage as f64) / state.cpu_total as f64;
            let memory_load_new = (memory_used + alloc.memory_usage as f64) / state.memory_total as f64;

            if best_cpu_load < cpu_load_new {
                if cpu_load_new < self.threshold && memory_load_new < self.threshold {
                    best_cpu_load = cpu_load_new;
                    result = Some(*host);
                }
            }
        }
        return result;
    }
}

// implement your host selection algorithm here
