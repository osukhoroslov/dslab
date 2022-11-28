//! Dot Product with resources weights algorithm.

use crate::core::common::Allocation;
use crate::core::common::AllocationVerdict;
use crate::core::monitoring::Monitoring;
use crate::core::resource_pool::ResourcePoolState;
use crate::core::vm_placement_algorithm::VMPlacementAlgorithm;

/// Dot product algorithm maximizes dot product between the vector of remaining capacities and the
/// vector of demands for the incoming VM capacities.
pub struct WeightedDotProduct;

impl WeightedDotProduct {
    pub fn new() -> Self {
        Self {}
    }
}

impl VMPlacementAlgorithm for WeightedDotProduct {
    fn select_host(&self, alloc: &Allocation, pool_state: &ResourcePoolState, _monitoring: &Monitoring) -> Option<u32> {
        let mut result: Option<u32> = None;
        let mut max_product: f64 = f64::MIN;

        for host in pool_state.get_hosts_list() {
            if pool_state.can_allocate(&alloc, host) == AllocationVerdict::Success {
                let cpu_product = pool_state.get_available_cpu(host) as f64 * alloc.cpu_usage as f64;
                let memory_product = pool_state.get_available_memory(host) as f64 * alloc.memory_usage as f64;
                let product = cpu_product / (pool_state.get_total_cpu(host) as f64).powi(2)
                    + memory_product / (pool_state.get_total_memory(host) as f64).powi(2);
                if product > max_product {
                    max_product = product;
                    result = Some(host);
                }
            }
        }
        result
    }
}
