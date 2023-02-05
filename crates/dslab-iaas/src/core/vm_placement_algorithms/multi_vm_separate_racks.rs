//! Multi VM packing algorithm which packs the VMs on different racks.

use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;

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
        return true;
    }
    !racks_occupied.contains(&pool.get_host(host).rack_id.unwrap())
}

impl MultiVMPlacementAlgorithm for SeparateRacksMultiVMPlacement {
    fn select_hosts(
        &self,
        allocs: Vec<Rc<RefCell<Allocation>>>,
        pool_state: &ResourcePoolState,
        _monitoring: &Monitoring,
    ) -> Option<Vec<u32>> {
        let mut result = Vec::<u32>::new();
        let mut pool = pool_state.clone();

        let mut racks_occupied = HashSet::<u32>::new();
        for alloc in allocs {
            let verdict: Option<u32> = pool.get_hosts_list().into_iter().find(|&host| {
                fits_by_rack(host, &racks_occupied, &pool)
                    && pool_state.can_allocate(&alloc.borrow(), host) == AllocationVerdict::Success
            });
            verdict?;

            if pool.get_host(verdict.unwrap()).rack_id.is_some() {
                racks_occupied.insert(pool.get_host(verdict.unwrap()).rack_id.unwrap());
            }

            result.push(verdict.unwrap());
            pool.allocate(&alloc.borrow(), verdict.unwrap());
        }
        Some(result)
    }
}
