//! First Fit algorithm.

use crate::core::common::Allocation;
use crate::core::common::AllocationVerdict;
use crate::core::monitoring::Monitoring;
use crate::core::resource_pool::ResourcePoolState;
use crate::core::vm_placement_algorithm::VMPlacementAlgorithm;

/// FirstFit algorithm, which returns the first suitable host.
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
