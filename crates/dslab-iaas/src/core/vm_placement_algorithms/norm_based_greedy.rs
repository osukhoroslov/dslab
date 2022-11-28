//! Norm-based Greedy algorithm.

use crate::core::common::Allocation;
use crate::core::common::AllocationVerdict;
use crate::core::monitoring::Monitoring;
use crate::core::resource_pool::ResourcePoolState;
use crate::core::vm_placement_algorithm::VMPlacementAlgorithm;

/// Norm-based Greedy algorithm minimizes the difference between the new VM resource usage
/// vector and the residual capacity under a certain norm, instead of the
/// dot product.
pub struct NormBasedGreedy;

impl NormBasedGreedy {
    pub fn new() -> Self {
        Self {}
    }
}

impl VMPlacementAlgorithm for NormBasedGreedy {
    fn select_host(&self, alloc: &Allocation, pool_state: &ResourcePoolState, _monitoring: &Monitoring) -> Option<u32> {
        let mut result: Option<u32> = None;
        let mut min_diff: f64 = f64::MAX;

        let mut cpu_weight = 0.;
        let mut memory_weight = 0.;
        for host in pool_state.get_hosts_list() {
            cpu_weight += pool_state.get_cpu_load(host);
            memory_weight += pool_state.get_memory_load(host);
        }
        cpu_weight /= pool_state.get_hosts_list().len() as f64;
        memory_weight /= pool_state.get_hosts_list().len() as f64;

        for host in pool_state.get_hosts_list() {
            if pool_state.can_allocate(&alloc, host) == AllocationVerdict::Success {
                let total_cpu: f64 = pool_state.get_total_cpu(host) as f64;
                let total_memory: f64 = pool_state.get_total_memory(host) as f64;

                // already normalized values
                let new_cpu: f64 = alloc.cpu_usage as f64 / total_cpu;
                let new_memory: f64 = alloc.memory_usage as f64 / total_memory;
                let load_cpu = 1. - pool_state.get_cpu_load(host);
                let load_memory = 1. - pool_state.get_memory_load(host);

                let cpu_diff = (new_cpu - load_cpu) * (new_cpu - load_cpu) * cpu_weight;
                let memory_diff = (new_memory - load_memory) * (new_memory - load_memory) * memory_weight;
                let diff = cpu_diff + memory_diff;
                if diff < min_diff {
                    min_diff = diff;
                    result = Some(host);
                }
            }
        }
        result
    }
}
