//! First Fit algorithm.

use crate::core::common::Allocation;
use crate::core::common::AllocationVerdict;
use crate::core::monitoring::Monitoring;
use crate::core::resource_pool::ResourcePoolState;
use crate::core::vm_placement_algorithm::SingleVMPlacementAlgorithm;

/// Uses the first suitable host.
#[derive(Default)]
pub struct FirstFit;

impl FirstFit {
    pub fn new() -> Self {
        Default::default()
    }
}

impl SingleVMPlacementAlgorithm for FirstFit {
    fn select_host(&self, alloc: &Allocation, pool_state: &ResourcePoolState, _monitoring: &Monitoring) -> Option<u32> {
        pool_state
            .get_host_ids()
            .into_iter()
            .find(|&host| pool_state.can_allocate(alloc, host, false) == AllocationVerdict::Success)
    }
}
