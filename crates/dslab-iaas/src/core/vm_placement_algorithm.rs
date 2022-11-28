//! Virtual machine placement algorithms.

use crate::core::common::Allocation;
use crate::core::config::parse_config_value;
use crate::core::monitoring::Monitoring;
use crate::core::resource_pool::ResourcePoolState;
use crate::core::vm_placement_algorithms::best_fit::BestFit;
use crate::core::vm_placement_algorithms::best_fit_threshold::BestFitThreshold;
use crate::core::vm_placement_algorithms::cosine_similarity::CosineSimilarity;
use crate::core::vm_placement_algorithms::dot_product::DotProduct;
use crate::core::vm_placement_algorithms::first_fit::FirstFit;
use crate::core::vm_placement_algorithms::norm_based_greedy::NormBasedGreedy;
use crate::core::vm_placement_algorithms::perp_distance::PerpDistance;
use crate::core::vm_placement_algorithms::weighted_dot_product::WeightedDotProduct;
use crate::core::vm_placement_algorithms::worst_fit::WorstFit;

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
        "CosineSimilarity" => return Box::new(CosineSimilarity::new()),
        "DotProduct" => return Box::new(DotProduct::new()),
        "WeightedDotProduct" => return Box::new(WeightedDotProduct::new()),
        "NormBasedGreedy" => return Box::new(NormBasedGreedy::new()),
        "PerpDistance" => return Box::new(PerpDistance::new()),
        _ => panic!("Can't resolve: {}", config_str),
    }
}
