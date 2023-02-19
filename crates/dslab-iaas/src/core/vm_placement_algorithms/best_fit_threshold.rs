//! Best Fit with threshold algorithm.

use crate::core::common::{Allocation, AllocationVerdict};
use crate::core::config::parse_options;
use crate::core::monitoring::Monitoring;
use crate::core::resource_pool::ResourcePoolState;
use crate::core::vm_placement_algorithm::SingleVMPlacementAlgorithm;

/// Uses the most loaded (by actual CPU load) suitable host.
/// The host CPU load after placement should be less than the specified `threshold`.
/// This algorithm can be used only in resource overcommitment mode.
pub struct BestFitThreshold {
    threshold: f64,
}

impl BestFitThreshold {
    pub fn new(threshold: f64) -> Self {
        Self { threshold }
    }

    pub fn from_string(s: &str) -> Self {
        let options = parse_options(s);
        let threshold = options.get("threshold").unwrap().parse::<f64>().unwrap();
        Self { threshold }
    }
}

impl Default for BestFitThreshold {
    fn default() -> Self {
        Self::new(1.0)
    }
}

impl SingleVMPlacementAlgorithm for BestFitThreshold {
    fn select_host(&self, alloc: &Allocation, pool_state: &ResourcePoolState, monitoring: &Monitoring) -> Option<u32> {
        let mut result: Option<u32> = None;
        let mut best_cpu_load: f64 = 0.;
        for host in monitoring.get_hosts_list() {
            if pool_state.can_allocate(alloc, *host, true) == AllocationVerdict::Success {
                let state = monitoring.get_host_state(*host);
                let cpu_used = state.cpu_load * state.cpu_total as f64;
                let cpu_load_new = (cpu_used + alloc.cpu_usage as f64) / state.cpu_total as f64;
                if best_cpu_load < cpu_load_new && cpu_load_new < self.threshold {
                    best_cpu_load = cpu_load_new;
                    result = Some(*host);
                }
            }
        }
        result
    }
}
