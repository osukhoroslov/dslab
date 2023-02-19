//! Worst Fit algorithm.

use crate::core::common::Allocation;
use crate::core::common::AllocationVerdict;
use crate::core::monitoring::Monitoring;
use crate::core::resource_pool::ResourcePoolState;
use crate::core::vm_placement_algorithm::SingleVMPlacementAlgorithm;

/// Uses the least loaded (by allocated CPU) suitable host.
#[derive(Default)]
pub struct WorstFit;

impl WorstFit {
    pub fn new() -> Self {
        Default::default()
    }
}

impl SingleVMPlacementAlgorithm for WorstFit {
    fn select_host(&self, alloc: &Allocation, pool_state: &ResourcePoolState, _monitoring: &Monitoring) -> Option<u32> {
        let mut result: Option<u32> = None;
        let mut max_available_cpu: u32 = 0;

        for host in pool_state.get_host_ids() {
            if pool_state.can_allocate(alloc, host) == AllocationVerdict::Success
                && pool_state.get_available_cpu(host) > max_available_cpu
            {
                max_available_cpu = pool_state.get_available_cpu(host);
                result = Some(host);
            }
        }
        result
    }
}
