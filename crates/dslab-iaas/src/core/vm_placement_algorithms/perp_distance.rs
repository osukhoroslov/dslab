//! Delta Perp Distance algorithm.

use crate::core::common::Allocation;
use crate::core::common::AllocationVerdict;
use crate::core::monitoring::Monitoring;
use crate::core::resource_pool::ResourcePoolState;
use crate::core::vm_placement_algorithm::VMPlacementAlgorithm;

/// Delta perp distance algorithm that minimizes the distance beetwen new allocated resources
/// vector point to host resource provider vector. As the result, VM with imbalanced resource
/// consupmtions are packed as tightly as possible and different resources fragmentation is
/// reduced significantly.
pub struct PerpDistance;

impl PerpDistance {
    pub fn new() -> Self {
        Self {}
    }
}

/// Get perp distance between {cpu, memory} vector and vector of host capacities
fn get_perp_distance(cpu: f64, memory: f64, host: u32, pool_state: &ResourcePoolState) -> f64 {
    let total_cpu: f64 = pool_state.get_total_cpu(host) as f64;
    let total_memory: f64 = pool_state.get_total_memory(host) as f64;
    let total_length = f64::sqrt(total_cpu.powi(2) + total_memory.powi(2));

    let length = f64::sqrt(cpu.powi(2) + memory.powi(2));
    if length == 0. {
        return 0.;
    }

    let scalar = total_cpu * cpu + total_memory * memory;
    let cos = scalar / total_length / length;
    let sin = f64::sqrt(1. - cos.powi(2));
    let distance = sin * length;
    distance
}

impl VMPlacementAlgorithm for PerpDistance {
    fn select_host(&self, alloc: &Allocation, pool_state: &ResourcePoolState, _monitoring: &Monitoring) -> Option<u32> {
        let mut result: Option<u32> = None;
        let mut max_delta: f64 = f64::MIN;

        for host in pool_state.get_hosts_list() {
            if pool_state.can_allocate(&alloc, host) == AllocationVerdict::Success {
                let allocated_cpu: f64 = pool_state.get_allocated_cpu(host) as f64;
                let allocated_memory: f64 = pool_state.get_allocated_memory(host) as f64;
                let distance_now = get_perp_distance(allocated_cpu, allocated_memory, host, pool_state);

                let new_cpu: f64 = pool_state.get_allocated_cpu(host) as f64 + alloc.cpu_usage as f64;
                let new_memory: f64 = pool_state.get_allocated_memory(host) as f64 + alloc.memory_usage as f64;
                let distance_new = get_perp_distance(new_cpu, new_memory, host, pool_state);

                println!("{} {}", distance_now, distance_new);

                let delta_perp_dist = distance_now - distance_new;
                if delta_perp_dist > max_delta {
                    max_delta = delta_perp_dist;
                    result = Some(host);
                }
            }
        }
        result
    }
}
