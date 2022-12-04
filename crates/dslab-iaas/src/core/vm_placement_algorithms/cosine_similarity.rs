//! Cosine Similarity algorithm.

use crate::core::common::Allocation;
use crate::core::common::AllocationVerdict;
use crate::core::monitoring::Monitoring;
use crate::core::resource_pool::ResourcePoolState;
use crate::core::vm_placement_algorithm::VMPlacementAlgorithm;

/// Maximizes the cosine of the angle between the host's resource usage and resource capacity vectors
/// after the allocation.
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
                let capacity_cpu = pool_state.get_total_cpu(host) as f64;
                let capacity_mem = pool_state.get_total_memory(host) as f64;
                let capacity_norm = (capacity_cpu.powi(2) + capacity_mem.powi(2)).sqrt();
                let usage_cpu = pool_state.get_allocated_cpu(host) as f64 + alloc.cpu_usage as f64;
                let usage_mem = pool_state.get_allocated_memory(host) as f64 + alloc.memory_usage as f64;
                let usage_norm = (usage_cpu.powi(2) + usage_mem.powi(2)).sqrt();
                let dot_product = capacity_cpu * usage_cpu + capacity_mem * usage_mem;
                let cosine = dot_product / (capacity_norm * usage_norm);
                if cosine > max_cosine {
                    max_cosine = cosine;
                    result = Some(host);
                }
            }
        }
        result
    }
}
