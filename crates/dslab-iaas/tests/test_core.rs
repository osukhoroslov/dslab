use dslab_core::simulation::Simulation;

use dslab_iaas::core::common::Allocation;
use dslab_iaas::core::config::SimulationConfig;
use dslab_iaas::core::load_model::ConstantLoadModel;
use dslab_iaas::core::monitoring::Monitoring;
use dslab_iaas::core::power_model::{ConstantPowerModel, HostPowerModel};
use dslab_iaas::core::resource_pool::ResourcePoolState;
use dslab_iaas::core::slav_metric::OverloadTimeFraction;
use dslab_iaas::core::vm::VmStatus;
use dslab_iaas::core::vm_placement_algorithm::VMPlacementAlgorithm;
use dslab_iaas::core::vm_placement_algorithms::best_fit::BestFit;
use dslab_iaas::core::vm_placement_algorithms::best_fit_threshold::BestFitThreshold;
use dslab_iaas::core::vm_placement_algorithms::first_fit::FirstFit;
use dslab_iaas::simulation::CloudSimulation;

fn name_wrapper(file_name: &str) -> String {
    format!("test-configs/{}", file_name)
}

#[test]
// Using default linear power model (0.4 + 0.6 * CPU load) with zero idle power.
// Host is loaded by 1/3 then power consumption is 0.4 + 0.6 / 3 = 0.6.
// VM lifetime is 2 seconds + 1 second of initializing + 0.5 seconds of shutdown.
// Thus, the overall energy consumed is (2 + 1 + 0.5) * 0.6 = 2.1.

fn test_energy_consumption() {
    let sim = Simulation::new(123);
    let sim_config = SimulationConfig::from_file(&name_wrapper("config.yaml"));
    let mut cloud_sim = CloudSimulation::new(sim, sim_config);

    let h = cloud_sim.add_host("h", 30, 30);
    let s = cloud_sim.add_scheduler("s", Box::new(BestFit::new()));

    cloud_sim.spawn_vm_now(
        10,
        10,
        2.0,
        Box::new(ConstantLoadModel::new(1.0)),
        Box::new(ConstantLoadModel::new(1.0)),
        None,
        s,
    );

    cloud_sim.step_for_duration(10.);
    let end_time = cloud_sim.current_time();

    assert_eq!(end_time, 10.);
    assert_eq!(cloud_sim.host(h).borrow_mut().get_energy_consumed(end_time), 2.1);
}

#[test]
// First fit selects first appropriate host.
fn test_first_fit() {
    let sim = Simulation::new(123);
    let sim_config = SimulationConfig::from_file(&name_wrapper("config.yaml"));
    let mut cloud_sim = CloudSimulation::new(sim, sim_config);

    let h1 = cloud_sim.add_host("h1", 100, 100);
    let h2 = cloud_sim.add_host("h2", 80, 80);
    let s = cloud_sim.add_scheduler("s", Box::new(FirstFit::new()));

    cloud_sim.spawn_vm_now(
        20,
        10,
        100.0,
        Box::new(ConstantLoadModel::new(1.0)),
        Box::new(ConstantLoadModel::new(1.0)),
        None,
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
        Box::new(ConstantLoadModel::new(1.0)),
        Box::new(ConstantLoadModel::new(1.0)),
        None,
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
// Best fit selects the host with the least free space left.
fn test_best_fit() {
    let sim = Simulation::new(123);
    let sim_config = SimulationConfig::from_file(&name_wrapper("config.yaml"));
    let mut cloud_sim = CloudSimulation::new(sim, sim_config);

    let h1 = cloud_sim.add_host("h1", 100, 100);
    let h2 = cloud_sim.add_host("h2", 80, 80);
    let s = cloud_sim.add_scheduler("s", Box::new(BestFit::new()));

    cloud_sim.spawn_vm_now(
        20,
        20,
        100.0,
        Box::new(ConstantLoadModel::new(1.0)),
        Box::new(ConstantLoadModel::new(1.0)),
        None,
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
        Box::new(ConstantLoadModel::new(1.0)),
        Box::new(ConstantLoadModel::new(1.0)),
        None,
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
// Not enough space for 11th VM, resources will be allocated on host 2.
fn test_no_overcommit() {
    let sim = Simulation::new(123);
    let sim_config = SimulationConfig::from_file(&name_wrapper("config.yaml"));
    let mut cloud_sim = CloudSimulation::new(sim, sim_config);

    let h1 = cloud_sim.add_host("h1", 100, 100);
    let h2 = cloud_sim.add_host("h2", 100, 100);
    let s = cloud_sim.add_scheduler("s", Box::new(BestFit::new()));

    for _ in 1..12 {
        cloud_sim.spawn_vm_now(
            10,
            10,
            100.0,
            Box::new(ConstantLoadModel::new(1.0)),
            Box::new(ConstantLoadModel::new(1.0)),
            None,
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
// Can pack 94 VMS despite their total SLA is 94 times bigger than host capacity.
fn test_overcommit() {
    let sim = Simulation::new(123);
    let sim_config = SimulationConfig::from_file(&name_wrapper("config_overcommit.yaml"));
    let mut cloud_sim = CloudSimulation::new(sim, sim_config);

    let h = cloud_sim.add_host("h", 200, 200);
    let s = cloud_sim.add_scheduler("s", Box::new(BestFitThreshold::new(1.0)));

    for _ in 1..95 {
        cloud_sim.spawn_vm_now(
            100,
            100,
            1000.0,
            Box::new(ConstantLoadModel::new(0.01)),
            Box::new(ConstantLoadModel::new(0.01)),
            None,
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
// User packs the VM on overloaded host, but the issue is resolved by placement store.
fn test_wrong_decision() {
    let sim = Simulation::new(123);
    let sim_config = SimulationConfig::from_file(&name_wrapper("config.yaml"));
    let mut cloud_sim = CloudSimulation::new(sim, sim_config);

    let h1 = cloud_sim.add_host("h1", 100, 100);
    let h2 = cloud_sim.add_host("h2", 100, 100);
    let s = cloud_sim.add_scheduler("s", Box::new(FirstFit::new()));

    let first_vm = cloud_sim.spawn_vm_now(
        100,
        100,
        100.0,
        Box::new(ConstantLoadModel::new(1.)),
        Box::new(ConstantLoadModel::new(1.)),
        None,
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
        Box::new(ConstantLoadModel::new(1.)),
        Box::new(ConstantLoadModel::new(1.)),
        None,
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
        Box::new(ConstantLoadModel::new(1.)),
        Box::new(ConstantLoadModel::new(1.)),
        None,
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
        Box::new(ConstantLoadModel::new(1.)),
        Box::new(ConstantLoadModel::new(1.)),
        None,
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
// Migrate a VM from host 1 to host 2.
// Network throughput is 10, then the migration of the VM with memory size 100 will take 10 seconds.
// The VM will finish at moment 21.4 (20 seconds + 1.4 for allocation).
// Due to asynchrony the status will be updated at moment 21.7.
fn test_migration_simple() {
    let sim = Simulation::new(123);
    let sim_config = SimulationConfig::from_file(&name_wrapper("config_overcommit.yaml"));
    let mut cloud_sim = CloudSimulation::new(sim, sim_config);

    let h1 = cloud_sim.add_host("h1", 200, 200);
    let h2 = cloud_sim.add_host("h2", 200, 200);
    let s = cloud_sim.add_scheduler("s", Box::new(FirstFit::new()));

    // VM spawns on host 1
    let vm = cloud_sim.spawn_vm_now(
        100,
        100,
        20.0,
        Box::new(ConstantLoadModel::new(1.)),
        Box::new(ConstantLoadModel::new(1.)),
        None,
        s,
    );

    cloud_sim.step_for_duration(5.);
    let mut current_time = cloud_sim.current_time();
    assert_eq!(current_time, 5.);
    assert_eq!(cloud_sim.host(h1).borrow_mut().get_cpu_load(current_time), 0.5);
    assert_eq!(cloud_sim.host(h1).borrow_mut().get_memory_load(current_time), 0.5);
    assert_eq!(cloud_sim.vm_status(vm), VmStatus::Running);
    assert_eq!(cloud_sim.vm_location(vm), h1);

    cloud_sim.migrate_vm_to_host(vm, h2);
    assert_eq!(cloud_sim.vm_status(vm), VmStatus::Running);
    assert_eq!(cloud_sim.vm_location(vm), h1);

    cloud_sim.step_for_duration(1.);
    assert_eq!(cloud_sim.vm_status(vm), VmStatus::Migrating);
    assert_eq!(cloud_sim.vm_location(vm), h1);

    cloud_sim.step_for_duration(5.);
    assert_eq!(cloud_sim.vm_status(vm), VmStatus::Migrating);
    assert_eq!(cloud_sim.vm_location(vm), h1);

    // Message delay 0.2 seconds makes the migration process little longer than 10 seconds
    cloud_sim.step_for_duration(5.);
    assert_eq!(cloud_sim.vm_status(vm), VmStatus::Migrating);
    assert_eq!(cloud_sim.vm_location(vm), h1);

    cloud_sim.step_for_duration(1.);
    assert_eq!(cloud_sim.vm_status(vm), VmStatus::Running);
    assert_eq!(cloud_sim.vm_location(vm), h2);

    current_time = cloud_sim.current_time();
    assert_eq!(current_time, 17.);
    assert_eq!(cloud_sim.host(h1).borrow_mut().get_cpu_load(current_time), 0.);
    assert_eq!(cloud_sim.host(h1).borrow_mut().get_memory_load(current_time), 0.);
    assert_eq!(cloud_sim.host(h2).borrow_mut().get_cpu_load(current_time), 0.5);
    assert_eq!(cloud_sim.host(h2).borrow_mut().get_memory_load(current_time), 0.5);

    cloud_sim.step_for_duration(4.69);
    current_time = cloud_sim.current_time();
    assert_eq!(current_time, 21.5); // due to there are no events between 21.5 and 21.7
    assert_eq!(cloud_sim.vm_status(vm), VmStatus::Running);

    cloud_sim.step_for_duration(0.2);
    current_time = cloud_sim.current_time();
    assert_eq!(current_time, 21.7);
    assert_eq!(cloud_sim.vm_status(vm), VmStatus::Finished);
}

#[test]
// Despite two migrations the VM will end at moment 101.7.
// (100 seconds of lifetime + 1.7 for asynchrony reasons like in previous test).
fn test_double_migration() {
    let sim = Simulation::new(123);
    let sim_config = SimulationConfig::from_file(&name_wrapper("config_overcommit.yaml"));
    let mut cloud_sim = CloudSimulation::new(sim, sim_config);

    let h1 = cloud_sim.add_host("h1", 200, 200);
    let h2 = cloud_sim.add_host("h2", 200, 200);
    let s = cloud_sim.add_scheduler("s", Box::new(FirstFit::new()));

    // VM spawns on host 1
    let vm = cloud_sim.spawn_vm_now(
        100,
        100,
        100.0,
        Box::new(ConstantLoadModel::new(1.)),
        Box::new(ConstantLoadModel::new(1.)),
        None,
        s,
    );

    // migration 1
    cloud_sim.step_for_duration(20.);
    cloud_sim.migrate_vm_to_host(vm, h2);

    // migration 2
    cloud_sim.step_for_duration(20.);
    cloud_sim.migrate_vm_to_host(vm, h1);

    cloud_sim.step_for_duration(60.);
    let mut current_time = cloud_sim.current_time();
    assert_eq!(current_time, 100.);
    assert_eq!(cloud_sim.vm_status(vm), VmStatus::Running);

    cloud_sim.step_for_duration(1.69);
    current_time = cloud_sim.current_time();
    assert_eq!(current_time, 101.5);
    assert_eq!(cloud_sim.vm_status(vm), VmStatus::Running);

    cloud_sim.step_for_duration(0.2);
    current_time = cloud_sim.current_time();
    assert_eq!(current_time, 101.7);
    assert_eq!(cloud_sim.vm_status(vm), VmStatus::Finished);
}

#[test]
// Default power model gets a result of 2.1 (test #1).
// Override the power model with constant of 1, then the total consumption is 3.5.
fn test_energy_consumption_override() {
    let sim = Simulation::new(123);
    let sim_config = SimulationConfig::from_file(&name_wrapper("config.yaml"));
    let mut cloud_sim = CloudSimulation::new(sim, sim_config);
    let power_model = HostPowerModel::new(Box::new(ConstantPowerModel::new(1.))).with_zero_idle_power();
    cloud_sim.set_host_power_model(power_model);

    let h = cloud_sim.add_host("h", 30, 30);
    let s = cloud_sim.add_scheduler("s", Box::new(BestFit::new()));

    cloud_sim.spawn_vm_now(
        10,
        10,
        2.0,
        Box::new(ConstantLoadModel::new(1.0)),
        Box::new(ConstantLoadModel::new(1.0)),
        None,
        s,
    );

    cloud_sim.step_for_duration(10.);
    let end_time = cloud_sim.current_time();

    assert_eq!(end_time, 10.);
    assert_eq!(cloud_sim.host(h).borrow_mut().get_energy_consumed(end_time), 3.5);
}

#[test]
// OTF metric is used to calculate SLA violation.
// Host is fully loaded then CPU load is 100%.
// Then during the period from 0 to 2 seconds host is fully loaded
// and is half-loaded between 2 and 4 seconds, thus the OTF metric is equal 50%.
fn test_slatah() {
    let sim = Simulation::new(123);
    let sim_config = SimulationConfig::from_file(&name_wrapper("config_zero_latency.yaml"));
    let mut cloud_sim = CloudSimulation::new(sim, sim_config);
    cloud_sim.set_slav_metric(Box::new(OverloadTimeFraction::new()));

    let h = cloud_sim.add_host("h", 40, 40);
    let s = cloud_sim.add_scheduler("s", Box::new(BestFit::new()));

    cloud_sim.spawn_vm_now(
        10,
        10,
        4.0,
        Box::new(ConstantLoadModel::new(2.0)),
        Box::new(ConstantLoadModel::new(2.0)),
        None,
        s,
    );
    cloud_sim.spawn_vm_now(
        10,
        10,
        2.0,
        Box::new(ConstantLoadModel::new(2.0)),
        Box::new(ConstantLoadModel::new(2.0)),
        None,
        s,
    );

    cloud_sim.step_for_duration(10.);
    let end_time = cloud_sim.current_time();

    assert_eq!(end_time, 10.);
    assert_eq!(cloud_sim.host(h).borrow_mut().get_accumulated_slav(end_time), 0.5);
}
