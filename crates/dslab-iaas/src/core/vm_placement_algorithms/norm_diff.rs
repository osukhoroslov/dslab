//! L2 Norm Diff algorithm.

use crate::core::common::Allocation;
use crate::core::common::AllocationVerdict;
use crate::core::monitoring::Monitoring;
use crate::core::resource_pool::ResourcePoolState;
use crate::core::vm_placement_algorithm::VMPlacementAlgorithm;

/// Minimizes the difference between the VM's resource usage and the host's available resources vectors
/// under the L^2 norm with additional resource weights.
/// The vectors are normalized to the host's capacity.
/// The resource weight corresponds to its average usage across all hosts.
pub struct L2NormDiff;

impl L2NormDiff {
    pub fn new() -> Self {
        Self {}
    }
}

impl VMPlacementAlgorithm for L2NormDiff {
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
                let total_cpu = pool_state.get_total_cpu(host);
                let total_memory = pool_state.get_total_memory(host);
                let available_cpu = pool_state.get_available_cpu(host);
                let available_memory = pool_state.get_available_memory(host);
                let cpu_diff = (available_cpu - alloc.cpu_usage) as f64 / total_cpu as f64;
                let memory_diff = (available_memory - alloc.memory_usage) as f64 / total_memory as f64;
                let diff = cpu_weight * cpu_diff.powi(2) + memory_weight * memory_diff.powi(2);
                if diff < min_diff {
                    min_diff = diff;
                    result = Some(host);
                }
            }
        }
        result
    }
}
