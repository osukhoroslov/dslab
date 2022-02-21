use crate::common::AllocationVerdict;
use crate::resource_pool::ResourcePoolState;
use crate::vm::VirtualMachine;

pub trait VMPlacementAlgorithm {
    fn select_host(&self, vm: &VirtualMachine, pool_state: &ResourcePoolState) -> Option<String>;
}

////////////////////////////////////////////////////////////////////////////////

pub struct FirstFit;

impl FirstFit {
    pub fn new() -> Self {
        Self {}
    }
}

impl VMPlacementAlgorithm for FirstFit {
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

pub struct BestFit;

impl BestFit {
    pub fn new() -> Self {
        Self {}
    }
}

impl VMPlacementAlgorithm for BestFit {
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

pub struct WorstFit;

impl WorstFit {
    pub fn new() -> Self {
        Self {}
    }
}

impl VMPlacementAlgorithm for WorstFit {
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
