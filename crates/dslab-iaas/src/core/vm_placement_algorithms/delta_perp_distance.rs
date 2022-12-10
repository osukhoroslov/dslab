//! Delta Perp-Distance algorithm.

use crate::core::common::Allocation;
use crate::core::common::AllocationVerdict;
use crate::core::monitoring::Monitoring;
use crate::core::resource_pool::ResourcePoolState;
use crate::core::vm_placement_algorithm::VMPlacementAlgorithm;

/// Maximizes the improvement (absolute decrease) of the perpendicular distance
/// between the host's resource usage and resource capacity vectors after the allocation.
pub struct DeltaPerpDistance;

impl DeltaPerpDistance {
    pub fn new() -> Self {
        Self {}
    }
}

impl VMPlacementAlgorithm for DeltaPerpDistance {
    fn select_host(&self, alloc: &Allocation, pool_state: &ResourcePoolState, _monitoring: &Monitoring) -> Option<u32> {
        let mut result: Option<u32> = None;
        let mut max_delta: f64 = f64::MIN;
        for host in pool_state.get_hosts_list() {
            if pool_state.can_allocate(&alloc, host) == AllocationVerdict::Success {
                let total_cpu = pool_state.get_total_cpu(host) as f64;
                let total_memory = pool_state.get_total_memory(host) as f64;
                let mut used_cpu = pool_state.get_allocated_cpu(host) as f64;
                let mut used_memory = pool_state.get_allocated_memory(host) as f64;
                let dist_before = perp_distance(used_cpu, used_memory, total_cpu, total_memory);
                used_cpu += alloc.cpu_usage as f64;
                used_memory += alloc.memory_usage as f64;
                let dist_after = perp_distance(used_cpu, used_memory, total_cpu, total_memory);
                let delta = dist_before - dist_after;
                if delta > max_delta {
                    max_delta = delta;
                    result = Some(host);
                }
            }
        }
        result
    }
}

/// Calculates perpendicular distance between {used_cpu, used_memory} and {total_cpu, total_memory} vectors.
fn perp_distance(used_cpu: f64, used_memory: f64, total_cpu: f64, total_memory: f64) -> f64 {
    if used_cpu == 0. && used_memory == 0. {
        return 0.;
    }
    let dot_product = total_cpu * used_cpu + total_memory * used_memory;
    let total_norm = (total_cpu.powi(2) + total_memory.powi(2)).sqrt();
    let used_norm = (used_cpu.powi(2) + used_memory.powi(2)).sqrt();
    let cos = dot_product / (total_norm * used_norm);
    let sin = (1. - cos.powi(2)).sqrt();
    used_norm * sin
}
