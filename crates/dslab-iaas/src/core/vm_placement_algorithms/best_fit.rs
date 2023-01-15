//! Best Fit algorithm.

use crate::core::common::Allocation;
use crate::core::common::AllocationVerdict;
use crate::core::monitoring::Monitoring;
use crate::core::resource_pool::ResourcePoolState;
use crate::core::vm_placement_algorithm::VMPlacementAlgorithm;

/// Uses the most loaded (by allocated CPU) suitable host.
#[derive(Default)]
pub struct BestFit;

impl BestFit {
    pub fn new() -> Self {
        Default::default()
    }
}

impl VMPlacementAlgorithm for BestFit {
    fn select_host(&self, alloc: &Allocation, pool_state: &ResourcePoolState, _monitoring: &Monitoring) -> Option<u32> {
        let mut result: Option<u32> = None;
        let mut min_available_cpu: u32 = u32::MAX;

        for host in pool_state.get_hosts_list() {
            if pool_state.can_allocate(alloc, host) == AllocationVerdict::Success
                && pool_state.get_available_cpu(host) < min_available_cpu
            {
                min_available_cpu = pool_state.get_available_cpu(host);
                result = Some(host);
            }
        }
        result
    }
}
