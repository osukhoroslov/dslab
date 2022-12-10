//! Best Fit with threshold algorithm.

use crate::core::common::Allocation;
use crate::core::config::parse_options;
use crate::core::monitoring::Monitoring;
use crate::core::resource_pool::ResourcePoolState;
use crate::core::vm_placement_algorithm::VMPlacementAlgorithm;

/// Uses the most loaded (by actual CPU load) suitable host.
/// The host load after placement should be less than the specified `threshold`.
/// This algorithm can be used only in resource overcommitment mode.
pub struct BestFitThreshold {
    threshold: f64,
}

impl BestFitThreshold {
    pub fn new(threshold: f64) -> Self {
        Self { threshold }
    }

    pub fn from_str(s: &str) -> Self {
        let options = parse_options(s);
        let threshold = options.get("threshold").unwrap().parse::<f64>().unwrap();
        Self { threshold }
    }
}

impl VMPlacementAlgorithm for BestFitThreshold {
    fn select_host(&self, alloc: &Allocation, _pool_state: &ResourcePoolState, monitoring: &Monitoring) -> Option<u32> {
        let mut result: Option<u32> = None;
        let mut best_cpu_load: f64 = 0.;
        for host in monitoring.get_hosts_list() {
            let state = monitoring.get_host_state(*host);
            let cpu_used = state.cpu_load * state.cpu_total as f64;
            let memory_used = state.memory_load * state.memory_total as f64;

            let cpu_load_new = (cpu_used + alloc.cpu_usage as f64) / state.cpu_total as f64;
            let memory_load_new = (memory_used + alloc.memory_usage as f64) / state.memory_total as f64;

            if best_cpu_load < cpu_load_new {
                if cpu_load_new < self.threshold && memory_load_new < self.threshold {
                    best_cpu_load = cpu_load_new;
                    result = Some(*host);
                }
            }
        }
        result
    }
}
