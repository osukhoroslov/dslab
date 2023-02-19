//! Rack anti-affinity algorithm.

use std::collections::HashSet;

use crate::core::common::{Allocation, AllocationVerdict};
use crate::core::monitoring::Monitoring;
use crate::core::resource_pool::ResourcePoolState;
use crate::core::vm_placement_algorithm::MultiVMPlacementAlgorithm;

/// Multi VM placement algorithm which places each VM from request on a separate rack.
/// First Fit is used as a basic algorithm for host selection.
#[derive(Default)]
pub struct RackAntiAffinity;

impl RackAntiAffinity {
    pub fn new() -> Self {
        Default::default()
    }
}

impl MultiVMPlacementAlgorithm for RackAntiAffinity {
    fn select_hosts(
        &self,
        allocations: &[Allocation],
        pool_state: &ResourcePoolState,
        _monitoring: &Monitoring,
    ) -> Option<Vec<u32>> {
        let mut result = Vec::new();
        let mut pool = pool_state.clone();
        let mut used_racks = HashSet::new();
        for alloc in allocations {
            if let Some(host) = pool.get_hosts().find(|&host| {
                !used_racks.contains(
                    &host
                        .rack_id
                        .expect("Rack is not set for host, cannot execute rack-aware placement algorithm"),
                ) && pool_state.can_allocate(alloc, host.id, false) == AllocationVerdict::Success
            }) {
                used_racks.insert(host.rack_id.unwrap());
                result.push(host.id);
                pool.allocate(alloc, host.id);
            } else {
                return None;
            }
        }
        Some(result)
    }
}
