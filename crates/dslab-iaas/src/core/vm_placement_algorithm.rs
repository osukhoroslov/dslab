//! Virtual machine placement algorithms.

use crate::core::common::Allocation;
use crate::core::config::options::parse_config_value;
use crate::core::monitoring::Monitoring;
use crate::core::resource_pool::ResourcePoolState;
use crate::core::vm_placement_algorithms::best_fit::BestFit;
use crate::core::vm_placement_algorithms::best_fit_threshold::BestFitThreshold;
use crate::core::vm_placement_algorithms::cosine_similarity::CosineSimilarity;
use crate::core::vm_placement_algorithms::delta_perp_distance::DeltaPerpDistance;
use crate::core::vm_placement_algorithms::dot_product::DotProduct;
use crate::core::vm_placement_algorithms::first_fit::FirstFit;
use crate::core::vm_placement_algorithms::norm_diff::L2NormDiff;
use crate::core::vm_placement_algorithms::rack_anti_affinity::RackAntiAffinity;
use crate::core::vm_placement_algorithms::weighted_dot_product::WeightedDotProduct;
use crate::core::vm_placement_algorithms::worst_fit::WorstFit;

pub enum VMPlacementAlgorithm {
    Single(Box<dyn SingleVMPlacementAlgorithm>),
    Multi(Box<dyn MultiVMPlacementAlgorithm>),
}

impl VMPlacementAlgorithm {
    pub fn single<T: SingleVMPlacementAlgorithm + 'static>(alg: T) -> Self {
        VMPlacementAlgorithm::Single(Box::new(alg))
    }

    pub fn multi<T: MultiVMPlacementAlgorithm + 'static>(alg: T) -> Self {
        VMPlacementAlgorithm::Multi(Box::new(alg))
    }
}

/// Trait for implementation of VM placement algorithms.
///
/// The algorithm is defined as a function of VM allocation request and current resource pool state, which returns an
/// ID of host selected for VM placement or `None` if there is not suitable host.
///
/// The reference to monitoring service is also passed to the algorithm so that it can use the information about
/// current host load.
pub trait SingleVMPlacementAlgorithm {
    fn select_host(&self, alloc: &Allocation, pool_state: &ResourcePoolState, monitoring: &Monitoring) -> Option<u32>;
}

/// Trait for implementation of multi-VM placement algorithms.
///
/// The algorithm is defined as a function of multiple VM allocation requests and current resource pool state,
/// which returns IDs of hosts selected for placement of each VM or `None` if there is no suitable placement.
///
/// The reference to monitoring service is also passed to the algorithm so that it can use the information about
/// current host load.
pub trait MultiVMPlacementAlgorithm {
    fn select_hosts(
        &self,
        allocations: &[Allocation],
        pool_state: &ResourcePoolState,
        monitoring: &Monitoring,
    ) -> Option<Vec<u32>>;
}

pub fn placement_algorithm_resolver(config_str: String) -> VMPlacementAlgorithm {
    let (algorithm_name, options) = parse_config_value(&config_str);
    match algorithm_name.as_str() {
        "FirstFit" => VMPlacementAlgorithm::single(FirstFit::new()),
        "BestFit" => VMPlacementAlgorithm::single(BestFit::new()),
        "WorstFit" => VMPlacementAlgorithm::single(WorstFit::new()),
        "BestFitThreshold" => VMPlacementAlgorithm::single(BestFitThreshold::from_string(&options.unwrap())),
        "CosineSimilarity" => VMPlacementAlgorithm::single(CosineSimilarity::new()),
        "DotProduct" => VMPlacementAlgorithm::single(DotProduct::new()),
        "WeightedDotProduct" => VMPlacementAlgorithm::single(WeightedDotProduct::new()),
        "L2NormDiff" => VMPlacementAlgorithm::single(L2NormDiff::new()),
        "DeltaPerpDistance" => VMPlacementAlgorithm::single(DeltaPerpDistance::new()),
        "RackAntiAffinity" => VMPlacementAlgorithm::multi(RackAntiAffinity::new()),
        _ => panic!("Can't resolve: {}", config_str),
    }
}
