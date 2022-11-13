//! Virtual machine placement algorithms.

use crate::core::common::Allocation;
use crate::core::common::AllocationVerdict;
use crate::core::config::parse_config_value;
use crate::core::config::parse_options;
use crate::core::monitoring::Monitoring;
use crate::core::resource_pool::ResourcePoolState;

/// Trait for implementation of VM placement algorithms.
///
/// The algorithm is defined as a function of VM allocation request and current resource pool state, which returns an
/// ID of host selected for VM placement or `None` if there is not suitable host.
///
/// The reference to monitoring service is also passed to the algorithm so that it can use the information about
/// current host load.
///
/// It is possible to implement arbitrary placement algorithm and use it in scheduler.
pub trait VMPlacementAlgorithm {
    fn select_host(&self, alloc: &Allocation, pool_state: &ResourcePoolState, monitoring: &Monitoring) -> Option<u32>;
}

pub fn placement_algorithm_resolver(config_str: String) -> Box<dyn VMPlacementAlgorithm> {
    let (algorithm_name, options) = parse_config_value(&config_str);
    match algorithm_name.as_str() {
        "FirstFit" => return Box::new(FirstFit::new()),
        "BestFit" => return Box::new(BestFit::new()),
        "WorstFit" => return Box::new(WorstFit::new()),
        "BestFitThreshold" => return Box::new(BestFitThreshold::from_str(&options.unwrap())),
        _ => panic!("Can't resolve: {}", config_str),
    }
}

////////////////////////////////////////////////////////////////////////////////

/// FirstFit algorithm, which returns the first suitable host.
pub struct FirstFit;

impl FirstFit {
    pub fn new() -> Self {
        Self {}
    }
}

impl VMPlacementAlgorithm for FirstFit {
    fn select_host(&self, alloc: &Allocation, pool_state: &ResourcePoolState, _monitoring: &Monitoring) -> Option<u32> {
        for host in pool_state.get_hosts_list() {
            if pool_state.can_allocate(&alloc, host) == AllocationVerdict::Success {
                return Some(host);
            }
        }
        return None;
    }
}

////////////////////////////////////////////////////////////////////////////////

/// BestFit algorithm, which returns the most loaded (by CPU) suitable host.
pub struct BestFit;

impl BestFit {
    pub fn new() -> Self {
        Self {}
    }
}

impl VMPlacementAlgorithm for BestFit {
    fn select_host(&self, alloc: &Allocation, pool_state: &ResourcePoolState, _monitoring: &Monitoring) -> Option<u32> {
        let mut result: Option<u32> = None;
        let mut min_available_cpu: u32 = u32::MAX;

        for host in pool_state.get_hosts_list() {
            if pool_state.can_allocate(&alloc, host) == AllocationVerdict::Success {
                if pool_state.get_available_cpu(host) < min_available_cpu {
                    min_available_cpu = pool_state.get_available_cpu(host);
                    result = Some(host);
                }
            }
        }
        return result;
    }
}

////////////////////////////////////////////////////////////////////////////////

/// WorstFit algorithm, which returns the least loaded (by CPU) suitable host.
pub struct WorstFit;

impl WorstFit {
    pub fn new() -> Self {
        Self {}
    }
}

impl VMPlacementAlgorithm for WorstFit {
    fn select_host(&self, alloc: &Allocation, pool_state: &ResourcePoolState, _monitoring: &Monitoring) -> Option<u32> {
        let mut result: Option<u32> = None;
        let mut max_available_cpu: u32 = 0;

        for host in pool_state.get_hosts_list() {
            if pool_state.can_allocate(&alloc, host) == AllocationVerdict::Success {
                if pool_state.get_available_cpu(host) > max_available_cpu {
                    max_available_cpu = pool_state.get_available_cpu(host);
                    result = Some(host);
                }
            }
        }
        return result;
    }
}

////////////////////////////////////////////////////////////////////////////////

/// BestFit algorithm, which returns the most loaded (by actual CPU load) suitable host.
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
        return result;
    }
}

////////////////////////////////////////////////////////////////////////////////

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

impl VMPlacementAlgorithm for PerpDistance {
    fn select_host(&self, alloc: &Allocation, pool_state: &ResourcePoolState, _monitoring: &Monitoring) -> Option<u32> {
        let mut result: Option<u32> = None;
        let mut min_perp_dist: f64 = f64::MAX;

        for host in pool_state.get_hosts_list() {
            if pool_state.can_allocate(&alloc, host) == AllocationVerdict::Success {
                let total_cpu: f64 = pool_state.get_total_cpu(host) as f64;
                let total_memory: f64 = pool_state.get_total_memory(host) as f64;
                let new_cpu: f64 = alloc.cpu_usage as f64;
                let new_memory: f64 = alloc.memory_usage as f64;

                let scalar: f64 = total_cpu * new_cpu + total_memory * new_memory;
                let dist_one = (total_cpu * total_cpu + total_memory * total_memory).sqrt();
                let dist_two = (new_cpu * new_cpu + new_memory * new_memory).sqrt();
                let cos = (scalar / dist_one) / dist_two;
                let sin = (1. - cos * cos).sqrt();

                let perp_dist = sin * dist_two;

                if perp_dist < min_perp_dist {
                    min_perp_dist = perp_dist;
                    result = Some(host);
                }
            }
        }
        result
    }
}

////////////////////////////////////////////////////////////////////////////////

/// Cosine similarity algorithm maximizes the a * d value, where a is a vector of currently
/// available host resources while d is new VM resource denamds. Both vectors are normialized
/// to host resource sizes.
pub struct CosineSimilarity;

impl CosineSimilarity {
    pub fn new() -> Self {
        Self {}
    }
}

impl VMPlacementAlgorithm for CosineSimilarity {
    fn select_host(&self, alloc: &Allocation, pool_state: &ResourcePoolState, _monitoring: &Monitoring) -> Option<u32> {
        let mut result: Option<u32> = None;
        let mut min_similarity: f64 = f64::MAX;

        for host in pool_state.get_hosts_list() {
            if pool_state.can_allocate(&alloc, host) == AllocationVerdict::Success {
                let total_cpu: f64 = pool_state.get_total_cpu(host) as f64;
                let total_memory: f64 = pool_state.get_total_memory(host) as f64;

                // already normalized values
                let new_cpu: f64 = alloc.cpu_usage as f64 / total_cpu;
                let new_memory: f64 = alloc.memory_usage as f64 / total_memory;
                let load_cpu = pool_state.get_cpu_load(host);
                let load_memory = pool_state.get_memory_load(host);

                let similarity = new_cpu * load_cpu + new_memory * load_memory;
                if similarity < min_similarity {
                    min_similarity = similarity;
                    result = Some(host);
                }
            }
        }
        result
    }
}

////////////////////////////////////////////////////////////////////////////////

/// Dot product algorithm maximizes dot product between the vector of remaining capacities and the
/// vector of demands for the incoming VM capacities.
pub struct DotProduct;

impl DotProduct {
    pub fn new() -> Self {
        Self {}
    }
}

impl VMPlacementAlgorithm for DotProduct {
    fn select_host(&self, alloc: &Allocation, pool_state: &ResourcePoolState, _monitoring: &Monitoring) -> Option<u32> {
        let mut result: Option<u32> = None;
        let mut max_product: f64 = f64::MIN;

        let mut cpu_weight = 0.;
        let mut memory_weight = 0.;
        for host in pool_state.get_hosts_list() {
            cpu_weight += pool_state.get_cpu_load(host);
            memory_weight += pool_state.get_memory_load(host);
        }
        cpu_weight /= pool_state.get_hosts_list().len() as f64;
        memory_weight /= pool_state.get_hosts_list().len() as f64;

        for host in pool_state.get_hosts_list() {
            if pool_state.can_allocate(&alloc, host) == AllocationVerdict::Success {
                let cpu_product = pool_state.get_available_cpu(host) as f64 * alloc.cpu_usage as f64 * cpu_weight;
                let memory_product =
                    pool_state.get_available_memory(host) as f64 * alloc.memory_usage as f64 * memory_weight;
                let product = cpu_product + memory_product;
                if product > max_product {
                    max_product = product;
                    result = Some(host);
                }
            }
        }
        result
    }
}

////////////////////////////////////////////////////////////////////////////////

/// Norm-based Greedy algorithm minimizes the difference between the new VM resource usage
/// vector and the residual capacity under a certain norm, instead of the
/// dot product.
pub struct NormBasedGreedy;

impl NormBasedGreedy {
    pub fn new() -> Self {
        Self {}
    }
}

impl VMPlacementAlgorithm for NormBasedGreedy {
    fn select_host(&self, alloc: &Allocation, pool_state: &ResourcePoolState, _monitoring: &Monitoring) -> Option<u32> {
        let mut result: Option<u32> = None;
        let mut min_diff: f64 = f64::MAX;

        let mut cpu_weight = 0.;
        let mut memory_weight = 0.;
        for host in pool_state.get_hosts_list() {
            cpu_weight += pool_state.get_cpu_load(host);
            memory_weight += pool_state.get_memory_load(host);
        }
        cpu_weight /= pool_state.get_hosts_list().len() as f64;
        memory_weight /= pool_state.get_hosts_list().len() as f64;

        for host in pool_state.get_hosts_list() {
            if pool_state.can_allocate(&alloc, host) == AllocationVerdict::Success {
                let total_cpu: f64 = pool_state.get_total_cpu(host) as f64;
                let total_memory: f64 = pool_state.get_total_memory(host) as f64;

                // already normalized values
                let new_cpu: f64 = alloc.cpu_usage as f64 / total_cpu;
                let new_memory: f64 = alloc.memory_usage as f64 / total_memory;
                let load_cpu = 1. - pool_state.get_cpu_load(host);
                let load_memory = 1. - pool_state.get_memory_load(host);

                let cpu_diff = (new_cpu - load_cpu) * (new_cpu - load_cpu) * cpu_weight;
                let memory_diff = (new_memory - load_memory) * (new_memory - load_memory) * memory_weight;
                let diff = cpu_diff + memory_diff;
                if diff < min_diff {
                    min_diff = diff;
                    result = Some(host);
                }
            }
        }
        result
    }
}
