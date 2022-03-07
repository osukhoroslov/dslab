use crate::common::AllocationVerdict;
use crate::monitoring::Monitoring;
use crate::resource_pool::Allocation;
use crate::resource_pool::ResourcePoolState;

pub trait VMPlacementAlgorithm {
    fn select_host(
        &self,
        alloc: &Allocation,
        pool_state: &ResourcePoolState,
        monitoring: &Monitoring,
    ) -> Option<String>;
}

////////////////////////////////////////////////////////////////////////////////

pub struct FirstFit;

impl FirstFit {
    pub fn new() -> Self {
        Self {}
    }
}

impl VMPlacementAlgorithm for FirstFit {
    fn select_host(
        &self,
        alloc: &Allocation,
        pool_state: &ResourcePoolState,
        _monitoring: &Monitoring,
    ) -> Option<String> {
        for host in pool_state.get_hosts_list() {
            if pool_state.can_allocate(&alloc, &host) == AllocationVerdict::Success {
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
    fn select_host(
        &self,
        alloc: &Allocation,
        pool_state: &ResourcePoolState,
        _monitoring: &Monitoring,
    ) -> Option<String> {
        let mut result: Option<String> = None;
        let mut best_cpu_load: f64 = 0.;

        for host in pool_state.get_hosts_list() {
            if pool_state.can_allocate(&alloc, &host) == AllocationVerdict::Success {
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
    fn select_host(
        &self,
        alloc: &Allocation,
        pool_state: &ResourcePoolState,
        _monitoring: &Monitoring,
    ) -> Option<String> {
        let mut result: Option<String> = None;
        let mut best_cpu_load: f64 = 0.;

        for host in pool_state.get_hosts_list() {
            if pool_state.can_allocate(&alloc, &host) == AllocationVerdict::Success {
                if result.is_none() || best_cpu_load > pool_state.get_cpu_load(&host) {
                    best_cpu_load = pool_state.get_cpu_load(&host);
                    result = Some(host);
                }
            }
        }
        return result;
    }
}

////////////////////////////////////////////////////////////////////////////////

pub struct BestFitThreshhold {
    threshhold: f64,
}

impl BestFitThreshhold {
    pub fn new(threshhold: f64) -> Self {
        Self { threshhold }
    }
}

impl VMPlacementAlgorithm for BestFitThreshhold {
    fn select_host(
        &self,
        alloc: &Allocation,
        _pool_state: &ResourcePoolState,
        monitoring: &Monitoring,
    ) -> Option<String> {
        let mut result: Option<String> = None;
        let mut best_cpu_load: f64 = 0.;
        for host in monitoring.get_hosts_list() {
            let state = monitoring.get_host_state(host);
            let cpu_used = state.cpu_load * state.cpu_total as f64;
            let memory_used = state.memory_load * state.memory_total as f64;

            let cpu_load_new = (cpu_used + alloc.cpu_usage as f64) / state.cpu_total as f64;
            let memory_load_new = (memory_used + alloc.memory_usage as f64) / state.memory_total as f64;

            if result.is_none() || best_cpu_load < cpu_load_new {
                if cpu_load_new < self.threshhold && memory_load_new < self.threshhold {
                    best_cpu_load = cpu_load_new;
                    result = Some(host.to_string());
                }
            }
        }
        return result;
    }
}
