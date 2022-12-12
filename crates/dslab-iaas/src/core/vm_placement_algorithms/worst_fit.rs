//! Worst fit algorithm.

use crate::core::common::Allocation;
use crate::core::common::AllocationVerdict;
use crate::core::monitoring::Monitoring;
use crate::core::resource_pool::ResourcePoolState;
use crate::core::vm_placement_algorithm::VMPlacementAlgorithm;

/// Uses the least loaded (by allocated CPU) suitable host.
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
        result
    }
}
