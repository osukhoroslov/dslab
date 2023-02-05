//! Dummy multi-VM packing algorithms. Just packs VM one-by-one using First Fit algorithm.

use std::cell::RefCell;
use std::rc::Rc;

use crate::core::common::Allocation;
use crate::core::monitoring::Monitoring;
use crate::core::resource_pool::ResourcePoolState;
use crate::core::vm_placement_algorithm::{MultiVMPlacementAlgorithm, SingleVMPlacementAlgorithm};
use crate::core::vm_placement_algorithms::first_fit::FirstFit;

/// Uses the first suitable host.
#[derive(Default)]
pub struct DummyMultiVMPlacement;

impl DummyMultiVMPlacement {
    pub fn new() -> Self {
        Default::default()
    }
}

impl MultiVMPlacementAlgorithm for DummyMultiVMPlacement {
    fn select_hosts(
        &self,
        allocs: Vec<Rc<RefCell<Allocation>>>,
        pool_state: &ResourcePoolState,
        monitoring: &Monitoring,
    ) -> Option<Vec<u32>> {
        let single_algo = FirstFit::new();
        let mut result = Vec::<u32>::new();
        let mut pool = pool_state.clone();

        for alloc in allocs {
            let verdict = single_algo.select_host(&alloc.borrow(), &pool, monitoring);
            verdict?;

            result.push(verdict.unwrap());
            pool.allocate(&alloc.borrow(), verdict.unwrap());
        }
        Some(result)
    }
}
