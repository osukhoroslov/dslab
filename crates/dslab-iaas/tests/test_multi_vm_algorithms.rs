use dslab_core::simulation::Simulation;

use dslab_iaas::core::config::SimulationConfig;
use dslab_iaas::core::vm::ResourceConsumer;
use dslab_iaas::core::vm_placement_algorithm::{MultiVMPlacementAlgorithm, VMPlacementAlgorithm};
use dslab_iaas::core::vm_placement_algorithms::multi_vm_dummy::DummyMultiVMPlacement;
use dslab_iaas::core::vm_placement_algorithms::multi_vm_separate_racks::SeparateRacksMultiVMPlacement;
use dslab_iaas::simulation::CloudSimulation;

// Runs the multi VM placement algorithm and checks its' placement decisions.
//
// The resource pool configuration and VMs sequence are chosen specially to show the difference between algorithms.
// The resource pool contains four hosts: two with 5 CPUs and 5GB of memory, and two with 8 CPUs and 4 GB of memory.
// The initial pool state consists of four VMs which are spawned directly on the hosts.
// There are three VMs spawned in the main stage, which should be placed by the algorithm.
// The hosts used for running each of these VMs are collected and compared to the expected hosts.
fn check_placements<T: MultiVMPlacementAlgorithm + 'static>(algorithm: T, expected_hosts: Vec<&str>) {
    let sim = Simulation::new(123);
    let sim_config = SimulationConfig::from_file("test-configs/config_zero_latency.yaml");
    let mut cloud_sim = CloudSimulation::new(sim, sim_config);

    let host_ids = vec![
        cloud_sim.add_host_inside_rack("h1", 10, 5, 0),
        cloud_sim.add_host_inside_rack("h2", 10, 5, 1),
        cloud_sim.add_host_inside_rack("h3", 13, 4, 2),
        cloud_sim.add_host_inside_rack("h4", 13, 4, 3),
    ];

    cloud_sim.spawn_vm_on_host(ResourceConsumer::with_full_load(6, 2), 10.0, None, host_ids[0]);
    cloud_sim.spawn_vm_on_host(ResourceConsumer::with_full_load(6, 2), 10.0, None, host_ids[1]);
    cloud_sim.spawn_vm_on_host(ResourceConsumer::with_full_load(7, 1), 10.0, None, host_ids[2]);
    cloud_sim.spawn_vm_on_host(ResourceConsumer::with_full_load(9, 1), 10.0, None, host_ids[3]);

    cloud_sim.step_for_duration(1.);

    let scheduler_id = cloud_sim.add_scheduler("s", VMPlacementAlgorithm::multi(algorithm));

    let vm_params = vec![
        ResourceConsumer::with_full_load(2, 2),
        ResourceConsumer::with_full_load(3, 1),
        ResourceConsumer::with_full_load(1, 1),
    ];
    let vm_ids = cloud_sim.spawn_vms_now(vm_params, 10.0, scheduler_id);

    cloud_sim.step_for_duration(1.);
    let cur_time = cloud_sim.current_time();
    assert_eq!(cur_time, 2.);

    for i in 0..vm_ids.len() {
        assert_eq!(cloud_sim.vm_location(vm_ids[i]), cloud_sim.lookup_id(expected_hosts[i]));
    }
}

#[test]
// Tests Dummy VM placement algorithm with First Fit as basic.
fn test_dummy_multi_first_fit() {
    check_placements(DummyMultiVMPlacement::new(), vec!["h1", "h2", "h1"]);
}

#[test]
// Tests different racks placement algorithm with First Fit as basic algorithm for host search.
// For the third VM host #3 is selected instead of #1 to satisfy the condition of different racks
fn test_different_racks_first_fit() {
    check_placements(SeparateRacksMultiVMPlacement::new(), vec!["h1", "h2", "h3"]);
}
