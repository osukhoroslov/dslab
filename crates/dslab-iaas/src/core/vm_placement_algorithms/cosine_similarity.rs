//! Cosine Similarity algorithm.

use crate::core::common::Allocation;
use crate::core::common::AllocationVerdict;
use crate::core::monitoring::Monitoring;
use crate::core::resource_pool::ResourcePoolState;
use crate::core::vm_placement_algorithm::VMPlacementAlgorithm;

/// Cosine similarity algorithm maximizes the cosine of angle between vector of host resources
/// capacities and vector of resource usages including incoming VM
pub struct CosineSimilarity;

impl CosineSimilarity {
    pub fn new() -> Self {
        Self {}
    }
}

impl VMPlacementAlgorithm for CosineSimilarity {
    fn select_host(&self, alloc: &Allocation, pool_state: &ResourcePoolState, _monitoring: &Monitoring) -> Option<u32> {
        let mut result: Option<u32> = None;
        let mut max_cosine: f64 = f64::MIN;

        for host in pool_state.get_hosts_list() {
            if pool_state.can_allocate(&alloc, host) == AllocationVerdict::Success {
                let total_cpu: f64 = pool_state.get_total_cpu(host) as f64;
                let total_memory: f64 = pool_state.get_total_memory(host) as f64;
                let length_total = f64::sqrt(total_cpu.powi(2) + total_memory.powi(2));

                let allocated_cpu: f64 = pool_state.get_allocated_cpu(host) as f64 + alloc.cpu_usage as f64;
                let allocated_memory: f64 = pool_state.get_allocated_memory(host) as f64 + alloc.memory_usage as f64;
                let length_allocated = f64::sqrt(allocated_cpu.powi(2) + allocated_memory.powi(2));

                let scalar = total_cpu * allocated_cpu + total_memory * allocated_memory;
                let cosine = scalar / length_total / length_allocated;
                if cosine > max_cosine {
                    max_cosine = cosine;
                    result = Some(host);
                }
            }
        }
        result
    }
}
