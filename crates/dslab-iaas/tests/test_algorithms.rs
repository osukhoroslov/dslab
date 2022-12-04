use dslab_core::simulation::Simulation;

use dslab_iaas::core::config::SimulationConfig;
use dslab_iaas::core::load_model::ConstantLoadModel;
use dslab_iaas::core::vm_placement_algorithms::cosine_similarity::CosineSimilarity;
use dslab_iaas::core::vm_placement_algorithms::delta_perp_distance::DeltaPerpDistance;
use dslab_iaas::core::vm_placement_algorithms::dot_product::DotProduct;
use dslab_iaas::core::vm_placement_algorithms::norm_diff::L2NormDiff;
use dslab_iaas::simulation::CloudSimulation;

fn name_wrapper(file_name: &str) -> String {
    format!("test-configs/{}", file_name)
}

// Configuration for algorithms tests, designed specially for showing their dirrerence.
// There are four hosts in simulation two with 5 CPUs and 5GB memory amount
// and two with 8 CPUs and 4 GB of memory.
// There are some virtual machnes spawned directly on hosts to build some initial configuration
// and skip the filling stage.
// There are three VMs spawned in main stage while the tests check only three of these allocations only.
// Function returns vector of VM IDs spawned during the main stage.
fn spawn_multiple_vms(cloud_sim: &mut CloudSimulation, scheduler_id: u32) -> Vec<u32> {
    let h1 = cloud_sim.add_host("h1", 5, 5);
    let h2 = cloud_sim.add_host("h2", 5, 5);
    let h3 = cloud_sim.add_host("h3", 8, 4);
    let h4 = cloud_sim.add_host("h4", 8, 4);

    cloud_sim.spawn_vm_directly(
        1,
        2,
        4.0,
        Box::new(ConstantLoadModel::new(1.0)),
        Box::new(ConstantLoadModel::new(1.0)),
        None,
        h1,
    );
    cloud_sim.spawn_vm_directly(
        1,
        2,
        4.0,
        Box::new(ConstantLoadModel::new(1.0)),
        Box::new(ConstantLoadModel::new(1.0)),
        None,
        h2,
    );
    cloud_sim.spawn_vm_directly(
        2,
        1,
        4.0,
        Box::new(ConstantLoadModel::new(1.0)),
        Box::new(ConstantLoadModel::new(1.0)),
        None,
        h3,
    );
    cloud_sim.spawn_vm_directly(
        4,
        1,
        4.0,
        Box::new(ConstantLoadModel::new(1.0)),
        Box::new(ConstantLoadModel::new(1.0)),
        None,
        h4,
    );

    let mut result = Vec::<u32>::new();
    result.push(cloud_sim.spawn_vm_now(
        2,
        2,
        4.0,
        Box::new(ConstantLoadModel::new(1.0)),
        Box::new(ConstantLoadModel::new(1.0)),
        None,
        scheduler_id,
    ));
    result.push(cloud_sim.spawn_vm_now(
        3,
        1,
        4.0,
        Box::new(ConstantLoadModel::new(1.0)),
        Box::new(ConstantLoadModel::new(1.0)),
        None,
        scheduler_id,
    ));
    result.push(cloud_sim.spawn_vm_now(
        1,
        1,
        4.0,
        Box::new(ConstantLoadModel::new(1.0)),
        Box::new(ConstantLoadModel::new(1.0)),
        None,
        scheduler_id,
    ));

    cloud_sim.step_for_duration(2.);
    let cur_time = cloud_sim.current_time();
    assert_eq!(cur_time, 2.);

    result
}

#[test]
// Test delta perp distance algorithm which minimizes the distance to host resource usage vector.
// Algorithm skips third host as long as a balance of resources is already achieved there.
fn test_delta_perp_dist() {
    let sim = Simulation::new(123);
    let sim_config = SimulationConfig::from_file(&name_wrapper("config_zero_latency.yaml"));
    let mut cloud_sim = CloudSimulation::new(sim, sim_config);
    let s = cloud_sim.add_scheduler("s", Box::new(DeltaPerpDistance::new()));

    let vms = spawn_multiple_vms(&mut cloud_sim, s);
    assert_eq!(
        cloud_sim.vm_location(vms[0]),
        cloud_sim.host_by_name("h4").borrow_mut().id
    );
    assert_eq!(
        cloud_sim.vm_location(vms[1]),
        cloud_sim.host_by_name("h1").borrow_mut().id
    );
    assert_eq!(
        cloud_sim.vm_location(vms[2]),
        cloud_sim.host_by_name("h2").borrow_mut().id
    );
}

#[test]
// Test cosine similarity algorithm.
// Third host is bigger than first and second thus the cosine is relatively smaller than
// other choices while dot product and perp distance are much bigger due to bigger linear sizes.
fn test_cosine_similarity() {
    let sim = Simulation::new(123);
    let sim_config = SimulationConfig::from_file(&name_wrapper("config_zero_latency.yaml"));
    let mut cloud_sim = CloudSimulation::new(sim, sim_config);
    let s = cloud_sim.add_scheduler("s", Box::new(CosineSimilarity::new()));

    let vms = spawn_multiple_vms(&mut cloud_sim, s);
    assert_eq!(
        cloud_sim.vm_location(vms[0]),
        cloud_sim.host_by_name("h4").borrow_mut().id
    );
    assert_eq!(
        cloud_sim.vm_location(vms[1]),
        cloud_sim.host_by_name("h3").borrow_mut().id
    );
    assert_eq!(
        cloud_sim.vm_location(vms[2]),
        cloud_sim.host_by_name("h3").borrow_mut().id
    );
}

#[test]
// Test dot product algorithm.
fn test_dot_product() {
    let sim = Simulation::new(123);
    let sim_config = SimulationConfig::from_file(&name_wrapper("config_zero_latency.yaml"));
    let mut cloud_sim = CloudSimulation::new(sim, sim_config);
    let s = cloud_sim.add_scheduler("s", Box::new(DotProduct::new()));

    let vms = spawn_multiple_vms(&mut cloud_sim, s);
    assert_eq!(
        cloud_sim.vm_location(vms[0]),
        cloud_sim.host_by_name("h3").borrow_mut().id
    );
    assert_eq!(
        cloud_sim.vm_location(vms[1]),
        cloud_sim.host_by_name("h1").borrow_mut().id
    );
    assert_eq!(
        cloud_sim.vm_location(vms[2]),
        cloud_sim.host_by_name("h2").borrow_mut().id
    );
}

#[test]
// Test L2 Norm Diff algorithm.
// Selects fourth host twice due to resources weights usage.
fn test_l2_norm_based_diff() {
    let sim = Simulation::new(123);
    let sim_config = SimulationConfig::from_file(&name_wrapper("config_zero_latency.yaml"));
    let mut cloud_sim = CloudSimulation::new(sim, sim_config);
    let s = cloud_sim.add_scheduler("s", Box::new(L2NormDiff::new()));

    let vms = spawn_multiple_vms(&mut cloud_sim, s);
    assert_eq!(
        cloud_sim.vm_location(vms[0]),
        cloud_sim.host_by_name("h4").borrow_mut().id
    );
    assert_eq!(
        cloud_sim.vm_location(vms[1]),
        cloud_sim.host_by_name("h1").borrow_mut().id
    );
    assert_eq!(
        cloud_sim.vm_location(vms[2]),
        cloud_sim.host_by_name("h4").borrow_mut().id
    );
}
