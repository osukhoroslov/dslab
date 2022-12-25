use dslab_core::simulation::Simulation;

use dslab_iaas::core::config::SimulationConfig;
use dslab_iaas::core::load_model::ConstantLoadModel;
use dslab_iaas::core::vm_placement_algorithm::VMPlacementAlgorithm;
use dslab_iaas::core::vm_placement_algorithms::best_fit::BestFit;
use dslab_iaas::core::vm_placement_algorithms::cosine_similarity::CosineSimilarity;
use dslab_iaas::core::vm_placement_algorithms::delta_perp_distance::DeltaPerpDistance;
use dslab_iaas::core::vm_placement_algorithms::dot_product::DotProduct;
use dslab_iaas::core::vm_placement_algorithms::first_fit::FirstFit;
use dslab_iaas::core::vm_placement_algorithms::norm_diff::L2NormDiff;
use dslab_iaas::core::vm_placement_algorithms::weighted_dot_product::WeightedDotProduct;
use dslab_iaas::core::vm_placement_algorithms::worst_fit::WorstFit;
use dslab_iaas::simulation::CloudSimulation;

// Runs the VM placement algorithm and checks its' placement decisions.
//
// The resource pool configuration and VMs sequence are chosen specially to show the difference between algorithms.
// The resource pool contains four hosts: two with 5 CPUs and 5GB of memory, and two with 8 CPUs and 4 GB of memory.
// The initial pool state consists of four VMs which are spawned directly on the hosts.
// There are three VMs spawned in the main stage, which should be placed by the algorithm.
// The hosts used for running each of these VMs are collected and compared to the expected hosts.
fn check_placements(algorithm: Box<dyn VMPlacementAlgorithm>, expected_hosts: Vec<&str>) {
    let sim = Simulation::new(123);
    let sim_config = SimulationConfig::from_file("test-configs/config_zero_latency.yaml");
    let mut cloud_sim = CloudSimulation::new(sim, sim_config);

    let mut host_ids = Vec::<u32>::new();
    host_ids.push(cloud_sim.add_host("h1", 10, 5));
    host_ids.push(cloud_sim.add_host("h2", 10, 5));
    host_ids.push(cloud_sim.add_host("h3", 13, 4));
    host_ids.push(cloud_sim.add_host("h4", 13, 4));

    cloud_sim.spawn_vm_on_host(
        6,
        2,
        10.0,
        Box::new(ConstantLoadModel::new(1.0)),
        Box::new(ConstantLoadModel::new(1.0)),
        None,
        host_ids[0],
    );
    cloud_sim.spawn_vm_on_host(
        6,
        2,
        10.0,
        Box::new(ConstantLoadModel::new(1.0)),
        Box::new(ConstantLoadModel::new(1.0)),
        None,
        host_ids[1],
    );
    cloud_sim.spawn_vm_on_host(
        7,
        1,
        10.0,
        Box::new(ConstantLoadModel::new(1.0)),
        Box::new(ConstantLoadModel::new(1.0)),
        None,
        host_ids[2],
    );
    cloud_sim.spawn_vm_on_host(
        9,
        1,
        10.0,
        Box::new(ConstantLoadModel::new(1.0)),
        Box::new(ConstantLoadModel::new(1.0)),
        None,
        host_ids[3],
    );

    cloud_sim.step_for_duration(1.);

    let scheduler_id = cloud_sim.add_scheduler("s", algorithm);

    let mut vm_ids = Vec::<u32>::new();
    vm_ids.push(cloud_sim.spawn_vm_now(
        2,
        2,
        10.0,
        Box::new(ConstantLoadModel::new(1.0)),
        Box::new(ConstantLoadModel::new(1.0)),
        None,
        scheduler_id,
    ));
    vm_ids.push(cloud_sim.spawn_vm_now(
        3,
        1,
        10.0,
        Box::new(ConstantLoadModel::new(1.0)),
        Box::new(ConstantLoadModel::new(1.0)),
        None,
        scheduler_id,
    ));
    vm_ids.push(cloud_sim.spawn_vm_now(
        1,
        1,
        10.0,
        Box::new(ConstantLoadModel::new(1.0)),
        Box::new(ConstantLoadModel::new(1.0)),
        None,
        scheduler_id,
    ));

    cloud_sim.step_for_duration(1.);
    let cur_time = cloud_sim.current_time();
    assert_eq!(cur_time, 2.);

    for i in 0..vm_ids.len() {
        assert_eq!(cloud_sim.vm_location(vm_ids[i]), cloud_sim.lookup_id(expected_hosts[i]));
    }
}

#[test]
// Tests FirstFit algorithm.
fn test_first_fit() {
    check_placements(Box::new(FirstFit::new()), vec!["h1", "h2", "h1"]);
}

#[test]
// Tests BestFit algorithm.
fn test_best_fit() {
    check_placements(Box::new(BestFit::new()), vec!["h1", "h2", "h2"]);
}

#[test]
// Tests WorstFit algorithm.
fn test_worst_fit() {
    check_placements(Box::new(WorstFit::new()), vec!["h3", "h1", "h2"]);
}

#[test]
// Tests Dot Product algorithm.
fn test_dot_product() {
    check_placements(Box::new(DotProduct::new()), vec!["h3", "h4", "h1"]);
}

#[test]
// Tests Weighted Dot Product algorithm.
fn test_weighted_dot_product() {
    check_placements(Box::new(WeightedDotProduct::new()), vec!["h3", "h1", "h4"]);
}

#[test]
// Tests L2 Norm Diff algorithm.
// Selects the fourth host twice due to resources weights usage.
fn test_l2_norm_diff() {
    check_placements(Box::new(L2NormDiff::new()), vec!["h4", "h1", "h4"]);
}

#[test]
// Tests Cosine Similarity algorithm.
// The third host is bigger than the first and the second ones thus the cosine is relatively smaller than
// the other choices while dot product and perp distance are much bigger due to bigger linear sizes.
fn test_cosine_similarity() {
    check_placements(Box::new(CosineSimilarity::new()), vec!["h1", "h3", "h3"]);
}

#[test]
// Tests Delta Perp-Distance algorithm.
// Algorithm skips the third host as long as the balance of resources is already achieved there.
fn test_delta_perp_distance() {
    check_placements(Box::new(DeltaPerpDistance::new()), vec!["h4", "h3", "h3"]);
}
