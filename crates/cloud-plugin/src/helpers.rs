pub mod load_functions {

    pub fn default_load_func(_timestamp: f64) -> f64 {
        1.
    }
}

pub mod allocation_policies {
    use crate::common::AllocationVerdict;
    use crate::resource_pool::ResourcePoolState;
    use crate::vm::VirtualMachine;

    pub fn first_fit(vm: &VirtualMachine, pool_state: &ResourcePoolState) -> Option<String> {
        for host in pool_state.get_hosts_list() {
            if pool_state.can_allocate(&vm, &host) == AllocationVerdict::Success {
                return Some(host.to_string());
            }
        }
        return None;
    }

    pub fn best_fit_by_cpu(vm: &VirtualMachine, pool_state: &ResourcePoolState) -> Option<String> {
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

    pub fn worst_fit_by_cpu(vm: &VirtualMachine, pool_state: &ResourcePoolState) -> Option<String> {
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
