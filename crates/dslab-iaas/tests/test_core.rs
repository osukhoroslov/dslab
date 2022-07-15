use dslab_core::simulation::Simulation;

use dslab_iaas::core::common::Allocation;
use dslab_iaas::core::config::SimulationConfig;
use dslab_iaas::core::load_model::ConstLoadModel;
use dslab_iaas::core::monitoring::Monitoring;
use dslab_iaas::core::resource_pool::ResourcePoolState;
use dslab_iaas::core::vm::VmStatus;
use dslab_iaas::core::vm_placement_algorithm::BestFit;
use dslab_iaas::core::vm_placement_algorithm::BestFitThreshold;
use dslab_iaas::core::vm_placement_algorithm::FirstFit;
use dslab_iaas::core::vm_placement_algorithm::VMPlacementAlgorithm;
use dslab_iaas::simulation::CloudSimulation;

fn name_wrapper(file_name: &str) -> String {
    format!("test-configs/{}", file_name)
}

#[test]
// Default enegrgy consumption function is 0.4 + 0.6 * CPU load
// Host is loaded by 1/3 then energy load is 0.4 + 0.6 / 3 = 0.6
// VM lifetime is 2 seconds + 1 second of initializing + 0.5 seconds of shutdown
// Thus, overall energy consumption is (2 + 1 + 0.5) * 0.6 = 2.1
fn test_energy_consumption() {
    let sim = Simulation::new(123);
    let sim_config = SimulationConfig::from_file(&name_wrapper("config.yaml"));
    let mut cloud_sim = CloudSimulation::new(sim, sim_config.clone());

    let h = cloud_sim.add_host("h", 30, 30);
    let s = cloud_sim.add_scheduler("s", Box::new(BestFit::new()));

    cloud_sim.spawn_vm_now(
        10,
        10,
        2.0,
        Box::new(ConstLoadModel::new(1.0)),
        Box::new(ConstLoadModel::new(1.0)),
        s,
    );

    cloud_sim.step_for_duration(10.);
    let end_time = cloud_sim.current_time();

    assert_eq!(end_time, 10.);
    assert_eq!(cloud_sim.host(h).borrow_mut().get_total_consumed(end_time), 2.1);
}

#[test]
// First fit selects first appropriate host
fn test_first_fit() {
    let sim = Simulation::new(123);
    let sim_config = SimulationConfig::from_file(&name_wrapper("config.yaml"));
    let mut cloud_sim = CloudSimulation::new(sim, sim_config.clone());

    let h1 = cloud_sim.add_host("h1", 100, 100);
    let h2 = cloud_sim.add_host("h2", 80, 80);
    let s = cloud_sim.add_scheduler("s", Box::new(FirstFit::new()));

    cloud_sim.spawn_vm_now(
        20,
        10,
        100.0,
        Box::new(ConstLoadModel::new(1.0)),
        Box::new(ConstLoadModel::new(1.0)),
        s,
    );

    cloud_sim.step_for_duration(5.);
    let mut current_time = cloud_sim.current_time();

    assert_eq!(current_time, 5.);
    assert_eq!(cloud_sim.host(h1).borrow_mut().get_cpu_load(current_time), 0.2);
    assert_eq!(cloud_sim.host(h2).borrow_mut().get_cpu_load(current_time), 0.);
    assert_eq!(cloud_sim.host(h1).borrow_mut().get_memory_load(current_time), 0.1);
    assert_eq!(cloud_sim.host(h2).borrow_mut().get_memory_load(current_time), 0.);

    cloud_sim.spawn_vm_now(
        20,
        20,
        100.0,
        Box::new(ConstLoadModel::new(1.0)),
        Box::new(ConstLoadModel::new(1.0)),
        s,
    );

    cloud_sim.step_for_duration(5.);
    current_time = cloud_sim.current_time();

    assert_eq!(current_time, 10.);
    assert_eq!(cloud_sim.host(h1).borrow_mut().get_cpu_load(current_time), 0.4);
    assert_eq!(cloud_sim.host(h2).borrow_mut().get_cpu_load(current_time), 0.);
    assert_eq!(cloud_sim.host(h1).borrow_mut().get_memory_load(current_time), 0.3);
    assert_eq!(cloud_sim.host(h2).borrow_mut().get_memory_load(current_time), 0.);
}

#[test]
// Best fit selects the host with the least free space left
fn test_best_fit() {
    let sim = Simulation::new(123);
    let sim_config = SimulationConfig::from_file(&name_wrapper("config.yaml"));
    let mut cloud_sim = CloudSimulation::new(sim, sim_config.clone());

    let h1 = cloud_sim.add_host("h1", 100, 100);
    let h2 = cloud_sim.add_host("h2", 80, 80);
    let s = cloud_sim.add_scheduler("s", Box::new(BestFit::new()));

    cloud_sim.spawn_vm_now(
        20,
        20,
        100.0,
        Box::new(ConstLoadModel::new(1.0)),
        Box::new(ConstLoadModel::new(1.0)),
        s,
    );

    cloud_sim.step_for_duration(5.);
    let mut current_time = cloud_sim.current_time();

    assert_eq!(current_time, 5.);
    assert_eq!(cloud_sim.host(h1).borrow_mut().get_cpu_load(current_time), 0.);
    assert_eq!(cloud_sim.host(h2).borrow_mut().get_cpu_load(current_time), 0.25);
    assert_eq!(cloud_sim.host(h1).borrow_mut().get_memory_load(current_time), 0.);
    assert_eq!(cloud_sim.host(h2).borrow_mut().get_memory_load(current_time), 0.25);

    cloud_sim.spawn_vm_now(
        20,
        20,
        100.0,
        Box::new(ConstLoadModel::new(1.0)),
        Box::new(ConstLoadModel::new(1.0)),
        s,
    );

    cloud_sim.step_for_duration(5.);
    current_time = cloud_sim.current_time();

    assert_eq!(current_time, 10.);
    assert_eq!(cloud_sim.host(h1).borrow_mut().get_cpu_load(current_time), 0.);
    assert_eq!(cloud_sim.host(h2).borrow_mut().get_cpu_load(current_time), 0.5);
    assert_eq!(cloud_sim.host(h1).borrow_mut().get_memory_load(current_time), 0.);
    assert_eq!(cloud_sim.host(h2).borrow_mut().get_memory_load(current_time), 0.5);
}

#[test]
// Not enough space for 11th VM, resources will be allocated on host 2
fn test_no_overcommit() {
    let sim = Simulation::new(123);
    let sim_config = SimulationConfig::from_file(&name_wrapper("config.yaml"));
    let mut cloud_sim = CloudSimulation::new(sim, sim_config.clone());

    let h1 = cloud_sim.add_host("h1", 100, 100);
    let h2 = cloud_sim.add_host("h2", 100, 100);
    let s = cloud_sim.add_scheduler("s", Box::new(BestFit::new()));

    for _i in 1..12 {
        cloud_sim.spawn_vm_now(
            10,
            10,
            100.0,
            Box::new(ConstLoadModel::new(1.0)),
            Box::new(ConstLoadModel::new(1.0)),
            s,
        );
        cloud_sim.step_for_duration(5.);
    }

    let current_time = cloud_sim.current_time();
    assert_eq!(current_time, 55.);
    assert_eq!(cloud_sim.host(h1).borrow_mut().get_cpu_load(current_time), 1.0);
    assert_eq!(cloud_sim.host(h2).borrow_mut().get_cpu_load(current_time), 0.1);
    assert_eq!(cloud_sim.host(h1).borrow_mut().get_memory_load(current_time), 1.0);
    assert_eq!(cloud_sim.host(h2).borrow_mut().get_memory_load(current_time), 0.1);
}

#[test]
// Can pack 94 VMS despite their total SLA is 94 times bigger than host capacity
fn test_overcommit() {
    let sim = Simulation::new(123);
    let sim_config = SimulationConfig::from_file(&name_wrapper("config_with_overcommit.yaml"));
    let mut cloud_sim = CloudSimulation::new(sim, sim_config.clone());

    let h = cloud_sim.add_host("h", 200, 200);
    let s = cloud_sim.add_scheduler("s", Box::new(BestFitThreshold::new(1.0)));

    for _i in 1..95 {
        cloud_sim.spawn_vm_now(
            100,
            100,
            1000.0,
            Box::new(ConstLoadModel::new(0.01)),
            Box::new(ConstLoadModel::new(0.01)),
            s,
        );
        cloud_sim.step_for_duration(1.);
    }

    cloud_sim.step_for_duration(5.);
    let current_time = cloud_sim.current_time();
    assert_eq!(current_time, 99.);
    assert_eq!(cloud_sim.host(h).borrow_mut().get_cpu_load(current_time), 0.47);
    assert_eq!(cloud_sim.host(h).borrow_mut().get_memory_load(current_time), 0.47);
}

pub struct BadScheduler {
    choice: u32,
}

impl BadScheduler {
    pub fn new(choice: u32) -> Self {
        Self { choice }
    }
}

impl VMPlacementAlgorithm for BadScheduler {
    fn select_host(
        &self,
        _alloc: &Allocation,
        _pool_state: &ResourcePoolState,
        _monitoring: &Monitoring,
    ) -> Option<u32> {
        return Some(self.choice);
    }
}

#[test]
// User packs the VM on overloaded host, but the issue is resolved by placement store
fn test_wrong_decision() {
    let sim = Simulation::new(123);
    let sim_config = SimulationConfig::from_file(&name_wrapper("config.yaml"));
    let mut cloud_sim = CloudSimulation::new(sim, sim_config.clone());

    let h1 = cloud_sim.add_host("h1", 100, 100);
    let h2 = cloud_sim.add_host("h2", 100, 100);
    let s = cloud_sim.add_scheduler("s", Box::new(FirstFit::new()));

    let first_vm = cloud_sim.spawn_vm_now(
        100,
        100,
        100.0,
        Box::new(ConstLoadModel::new(1.)),
        Box::new(ConstLoadModel::new(1.)),
        s,
    );
    cloud_sim.step_for_duration(5.);

    // now host one is overloaded
    let mut current_time = cloud_sim.current_time();
    assert_eq!(current_time, 5.);
    assert_eq!(cloud_sim.host(h1).borrow_mut().get_cpu_load(current_time), 1.);
    assert_eq!(cloud_sim.host(h1).borrow_mut().get_memory_load(current_time), 1.);
    assert_eq!(cloud_sim.vm_status(first_vm), VmStatus::Running);

    let bad_s = cloud_sim.add_scheduler("bad_s", Box::new(BadScheduler::new(h1)));
    let second_vm = cloud_sim.spawn_vm_now(
        100,
        100,
        100.0,
        Box::new(ConstLoadModel::new(1.)),
        Box::new(ConstLoadModel::new(1.)),
        bad_s,
    );
    cloud_sim.step_for_duration(5.);
    current_time = cloud_sim.current_time();

    assert_eq!(current_time, 10.);
    assert_eq!(cloud_sim.host(h1).borrow_mut().get_cpu_load(current_time), 1.);
    assert_eq!(cloud_sim.host(h2).borrow_mut().get_cpu_load(current_time), 0.);
    assert_eq!(cloud_sim.host(h1).borrow_mut().get_memory_load(current_time), 1.);
    assert_eq!(cloud_sim.host(h2).borrow_mut().get_memory_load(current_time), 0.);
    assert_eq!(cloud_sim.vm_status(second_vm), VmStatus::Initializing);

    // now host does not exist
    let random_wrong_id = 47;
    let bad_s2 = cloud_sim.add_scheduler("bad_s2", Box::new(BadScheduler::new(random_wrong_id)));
    let third_vm = cloud_sim.spawn_vm_now(
        100,
        100,
        100.0,
        Box::new(ConstLoadModel::new(1.)),
        Box::new(ConstLoadModel::new(1.)),
        bad_s2,
    );
    cloud_sim.step_for_duration(5.);
    current_time = cloud_sim.current_time();

    assert_eq!(current_time, 15.);
    assert_eq!(cloud_sim.host(h1).borrow_mut().get_cpu_load(current_time), 1.);
    assert_eq!(cloud_sim.host(h2).borrow_mut().get_cpu_load(current_time), 0.);
    assert_eq!(cloud_sim.host(h1).borrow_mut().get_memory_load(current_time), 1.);
    assert_eq!(cloud_sim.host(h2).borrow_mut().get_memory_load(current_time), 0.);
    assert_eq!(cloud_sim.vm_status(third_vm), VmStatus::Initializing);

    // finally right decision
    let fine_s = cloud_sim.add_scheduler("fine_s", Box::new(BadScheduler::new(h2)));
    let fourth_vm = cloud_sim.spawn_vm_now(
        100,
        100,
        100.0,
        Box::new(ConstLoadModel::new(1.)),
        Box::new(ConstLoadModel::new(1.)),
        fine_s,
    );
    cloud_sim.step_for_duration(5.);
    current_time = cloud_sim.current_time();

    assert_eq!(current_time, 20.);
    assert_eq!(cloud_sim.host(h1).borrow_mut().get_cpu_load(current_time), 1.);
    assert_eq!(cloud_sim.host(h2).borrow_mut().get_cpu_load(current_time), 1.);
    assert_eq!(cloud_sim.host(h1).borrow_mut().get_memory_load(current_time), 1.);
    assert_eq!(cloud_sim.host(h2).borrow_mut().get_memory_load(current_time), 1.);
    assert_eq!(cloud_sim.vm_status(fourth_vm), VmStatus::Running);

    cloud_sim.step_for_duration(100.);
    assert_eq!(cloud_sim.vm_status(first_vm), VmStatus::Finished);
    assert_eq!(cloud_sim.vm_status(second_vm), VmStatus::FailedToAllocate);
    assert_eq!(cloud_sim.vm_status(third_vm), VmStatus::FailedToAllocate);
    assert_eq!(cloud_sim.vm_status(fourth_vm), VmStatus::Finished);
}

#[test]
// Migrate a VM from host 1 to host 2
// Network throughput is 10, then the migration of the VM with memory size 100 will take 10 seconds
fn test_migration_simple() {
    let sim = Simulation::new(123);
    let sim_config = SimulationConfig::from_file(&name_wrapper("config_with_overcommit.yaml"));
    let mut cloud_sim = CloudSimulation::new(sim, sim_config.clone());

    let h1 = cloud_sim.add_host("h1", 200, 200);
    let h2 = cloud_sim.add_host("h2", 200, 200);
    let s = cloud_sim.add_scheduler("s", Box::new(FirstFit::new()));

    // VM spawns on host 1
    let vm = cloud_sim.spawn_vm_now(
        100,
        100,
        1000.0,
        Box::new(ConstLoadModel::new(1.)),
        Box::new(ConstLoadModel::new(1.)),
        s,
    );

    cloud_sim.step_for_duration(5.);
    let mut current_time = cloud_sim.current_time();
    assert_eq!(current_time, 5.);
    assert_eq!(cloud_sim.host(h1).borrow_mut().get_cpu_load(current_time), 0.5);
    assert_eq!(cloud_sim.host(h1).borrow_mut().get_memory_load(current_time), 0.5);
    assert_eq!(cloud_sim.vm_status(vm), VmStatus::Running);

    cloud_sim.migrate_vm_to_host(vm, h2);
    assert_eq!(cloud_sim.vm_status(vm), VmStatus::Running);

    cloud_sim.step_for_duration(1.);
    assert_eq!(cloud_sim.vm_status(vm), VmStatus::Migrating);

    cloud_sim.step_for_duration(5.);
    assert_eq!(cloud_sim.vm_status(vm), VmStatus::Migrating);

    // Message delay 0.2 seconds makes the migration process little longer than 10 seconds
    cloud_sim.step_for_duration(5.);
    assert_eq!(cloud_sim.vm_status(vm), VmStatus::Migrating);

    cloud_sim.step_for_duration(1.);
    assert_eq!(cloud_sim.vm_status(vm), VmStatus::Running);

    current_time = cloud_sim.current_time();
    assert_eq!(current_time, 17.);
    assert_eq!(cloud_sim.host(h1).borrow_mut().get_cpu_load(current_time), 0.);
    assert_eq!(cloud_sim.host(h1).borrow_mut().get_memory_load(current_time), 0.);
    assert_eq!(cloud_sim.host(h2).borrow_mut().get_cpu_load(current_time), 0.5);
    assert_eq!(cloud_sim.host(h2).borrow_mut().get_memory_load(current_time), 0.5);
}
