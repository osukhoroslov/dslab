use std::fmt;

use dyn_clone::DynClone;
use std::fmt::Debug;

use crate::common::AllocationVerdict;
use crate::resource_pool::ResourcePoolState;
use crate::vm::VirtualMachine;

pub trait VMPlacementAlgorithm: DynClone {
    fn init(&mut self);
    fn select_host(&self, vm: &VirtualMachine, pool_state: &ResourcePoolState) -> Option<String>;
}

impl fmt::Debug for dyn VMPlacementAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("").finish()
    }
}

dyn_clone::clone_trait_object!(VMPlacementAlgorithm);

////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct FirstFitVMPlacement;

impl FirstFitVMPlacement {
    pub fn new() -> Self {
        Self {}
    }
}

impl VMPlacementAlgorithm for FirstFitVMPlacement {
    fn init(&mut self) {}

    fn select_host(&self, vm: &VirtualMachine, pool_state: &ResourcePoolState) -> Option<String> {
        for host in pool_state.get_hosts_list() {
            if pool_state.can_allocate(&vm, &host) == AllocationVerdict::Success {
                return Some(host.to_string());
            }
        }
        return None;
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct BestFitVMPlacement;

impl BestFitVMPlacement {
    pub fn new() -> Self {
        Self {}
    }
}

impl VMPlacementAlgorithm for BestFitVMPlacement {
    fn init(&mut self) {}

    fn select_host(&self, vm: &VirtualMachine, pool_state: &ResourcePoolState) -> Option<String> {
        let mut result: Option<String> = None;
        let mut best_cpu_load: f64 = 0.;

        for host in pool_state.get_hosts_list() {
            if pool_state.can_allocate(&vm, &host) == AllocationVerdict::Success {
                if result.is_none() || best_cpu_load < pool_state.get_cpu_load(&host) {
                    best_cpu_load = pool_state.get_cpu_load(&host);
                    result = Some(host);
                }
            }
        }
        return result;
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct WorstFitVMPlacement;

impl WorstFitVMPlacement {
    pub fn new() -> Self {
        Self {}
    }
}

impl VMPlacementAlgorithm for WorstFitVMPlacement {
    fn init(&mut self) {}

    fn select_host(&self, vm: &VirtualMachine, pool_state: &ResourcePoolState) -> Option<String> {
        let mut result: Option<String> = None;
        let mut best_cpu_load: f64 = 0.;

        for host in pool_state.get_hosts_list() {
            if pool_state.can_allocate(&vm, &host) == AllocationVerdict::Success {
                if result.is_none() || best_cpu_load > pool_state.get_cpu_load(&host) {
                    best_cpu_load = pool_state.get_cpu_load(&host);
                    result = Some(host);
                }
            }
        }
        return result;
    }
}
