//! Multi VM packing algorithm which packs the VMs on different racks.

use std::collections::HashSet;

use crate::core::common::{Allocation, AllocationVerdict};
use crate::core::monitoring::Monitoring;
use crate::core::resource_pool::ResourcePoolState;
use crate::core::vm_placement_algorithm::MultiVMPlacementAlgorithm;

/// Multi VM packing algorithm which packs the VMs on different racks.
/// First Fit algorithm is used as basic algorithm for host selection.
#[derive(Default)]
pub struct SeparateRacksMultiVMPlacement;

impl SeparateRacksMultiVMPlacement {
    pub fn new() -> Self {
        Default::default()
    }
}

fn fits_by_rack(host: u32, racks_occupied: &HashSet<u32>, pool: &ResourcePoolState) -> bool {
    if pool.get_host(host).rack_id.is_none() {
        panic!(
            "Rack is not set for host {}, cannot execute rack-aware placement algorithm",
            host
        );
    }
    !racks_occupied.contains(&pool.get_host(host).rack_id.unwrap())
}

impl MultiVMPlacementAlgorithm for SeparateRacksMultiVMPlacement {
    fn select_hosts(
        &self,
        allocs: &[Allocation],
        pool_state: &ResourcePoolState,
        _monitoring: &Monitoring,
    ) -> Option<Vec<u32>> {
        let mut result = Vec::<u32>::new();
        let mut pool = pool_state.clone();

        let mut racks_occupied = HashSet::<u32>::new();
        for alloc in allocs {
            let verdict: Option<u32> = pool.get_hosts_list().into_iter().find(|&host| {
                fits_by_rack(host, &racks_occupied, &pool)
                    && pool_state.can_allocate(&alloc, host) == AllocationVerdict::Success
            });
            verdict?;

            racks_occupied.insert(pool.get_host(verdict.unwrap()).rack_id.unwrap());
            result.push(verdict.unwrap());
            pool.allocate(&alloc, verdict.unwrap());
        }
        Some(result)
    }
}
