use log::info;

use dslab_core::simulation::Simulation;
use dslab_iaas::core::config::SimulationConfig;
use dslab_iaas::core::load_model::ConstantLoadModel;
use dslab_iaas::core::load_model::LoadModel;
use dslab_iaas::core::vm_placement_algorithm::BestFit;
use dslab_iaas::custom_component::CustomComponent;
use dslab_iaas::extensions::standard_dataset_reader::StandardDatasetReader;
use dslab_iaas::extensions::vm_migrator::VmMigrator;
use dslab_iaas::simulation::CloudSimulation;

fn init_logger() {
    use env_logger::Builder;
    use std::io::Write;
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();
}

fn simulation_two_best_fit_schedulers(sim_config: SimulationConfig) {
    let sim = Simulation::new(123);
    let mut cloud_sim = CloudSimulation::new(sim, sim_config);

    let h1 = cloud_sim.add_host("h1", 30, 30);
    let h2 = cloud_sim.add_host("h2", 30, 30);
    let s1 = cloud_sim.add_scheduler("s1", Box::new(BestFit::new()));
    let s2 = cloud_sim.add_scheduler("s2", Box::new(BestFit::new()));

    // spawn vm_0 - vm_4 on scheduler #1
    for _ in 0..5 {
        cloud_sim.spawn_vm_now(
            10,
            10,
            2.0,
            Box::new(ConstantLoadModel::new(1.0)),
            Box::new(ConstantLoadModel::new(1.0)),
            None,
            s1,
        );
    }
    // spawn vm_5 - vm_9 on scheduler #2
    for _ in 5..10 {
        cloud_sim.spawn_vm_now(
            10,
            10,
            2.0,
            Box::new(ConstantLoadModel::new(1.0)),
            Box::new(ConstantLoadModel::new(1.0)),
            None,
            s2,
        );
    }

    // spawn vm_10 - vm_14 on scheduler #1
    for _ in 10..15 {
        cloud_sim.spawn_vm_now(
            10,
            10,
            2.0,
            Box::new(ConstantLoadModel::new(1.0)),
            Box::new(ConstantLoadModel::new(1.0)),
            None,
            s1,
        );
    }
    // spawn vm_15 - vm_19 on scheduler #2
    for _ in 15..20 {
        cloud_sim.spawn_vm_now(
            10,
            10,
            2.0,
            Box::new(ConstantLoadModel::new(1.0)),
            Box::new(ConstantLoadModel::new(1.0)),
            None,
            s2,
        );
    }

    cloud_sim.steps(500);

    let end_time = cloud_sim.current_time();
    info!(
        "Total energy consumed by host one: {}",
        cloud_sim.host(h1).borrow_mut().get_energy_consumed(end_time)
    );
    info!(
        "Total energy consumed by host two: {}",
        cloud_sim.host(h2).borrow_mut().get_energy_consumed(end_time)
    );
}

////////////////////////////////////////////////////////////////////////////////

fn simulation_one_thresholded_scheduler(sim_config: SimulationConfig) {
    let sim = Simulation::new(123);
    let mut cloud_sim = CloudSimulation::new(sim, sim_config);
    let scheduler_id = cloud_sim.lookup_id("s");

    let mut dataset = StandardDatasetReader::new();
    dataset.parse("workload.json");

    cloud_sim.spawn_vms_from_dataset(scheduler_id, &mut dataset);

    cloud_sim.steps(300);

    let end_time = cloud_sim.current_time();
    info!(
        "Total energy consumed by host one: {}",
        cloud_sim.host_by_name("h1").borrow_mut().get_energy_consumed(end_time)
    );
    info!(
        "Total energy consumed by host two: {}",
        cloud_sim.host_by_name("h2").borrow_mut().get_energy_consumed(end_time)
    );
}

////////////////////////////////////////////////////////////////////////////////

fn simulation_migration_simple(sim_config: SimulationConfig) {
    let sim = Simulation::new(123);
    let mut cloud_sim = CloudSimulation::new(sim, sim_config);

    let h1 = cloud_sim.add_host("h1", 30, 30);
    let h2 = cloud_sim.add_host("h2", 30, 30);
    let scheduler_id = cloud_sim.add_scheduler("s", Box::new(BestFit::new()));

    let vm = cloud_sim.spawn_vm_now(
        10,
        10,
        20.0,
        Box::new(ConstantLoadModel::new(0.5)),
        Box::new(ConstantLoadModel::new(0.5)),
        None,
        scheduler_id,
    );

    cloud_sim.step_for_duration(10.);
    cloud_sim.migrate_vm_to_host(vm, h2);

    cloud_sim.steps(300);

    let end_time = cloud_sim.current_time();
    info!(
        "Total energy consumed by host one: {}",
        cloud_sim.host(h1).borrow_mut().get_energy_consumed(end_time)
    );
    info!(
        "Total energy consumed by host two: {}",
        cloud_sim.host(h2).borrow_mut().get_energy_consumed(end_time)
    );
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct DecreaseLoadModel {}

impl DecreaseLoadModel {
    pub fn new() -> Self {
        Self {}
    }
}

impl LoadModel for DecreaseLoadModel {
    fn get_resource_load(&self, time: f64, _time_from_start: f64) -> f64 {
        // linear drop from 100% to zero within first 50 time points
        // then linear growth back from zero to 100% during next 50 time points
        if time <= 500. {
            let linear = (500. - time) / 500.;
            return linear.max(0.);
        } else {
            let linear = (time - 500.) / 500.;
            return linear.min(1.);
        }
    }
}

fn simulation_migration_component(sim_config: SimulationConfig) {
    let sim = Simulation::new(123);
    let mut cloud_sim = CloudSimulation::new(sim, sim_config);

    let scheduler_id = cloud_sim.add_scheduler("s", Box::new(BestFit::new()));

    for i in 0..10 {
        cloud_sim.add_host(&format!("h{}", i), 50, 50);
    }

    for _ in 0..10 {
        cloud_sim.spawn_vm_now(
            30,
            30,
            1000.0,
            Box::new(DecreaseLoadModel::new()),
            Box::new(DecreaseLoadModel::new()),
            None,
            scheduler_id,
        );
    }

    let migrator = cloud_sim.build_custom_component::<VmMigrator>("migrator");
    migrator
        .borrow_mut()
        .patch_custom_args(5., cloud_sim.monitoring(), cloud_sim.vm_api(), cloud_sim.sim_config());
    migrator.borrow_mut().init();

    cloud_sim.step_for_duration(1005.);
}

fn main() {
    init_logger();
    let config = SimulationConfig::from_file("config.yaml");
    let config_overcommit = SimulationConfig::from_file("config_overcommit.yaml");
    let config_with_infra = SimulationConfig::from_file("config_with_infrastructure.yaml");
    simulation_two_best_fit_schedulers(config.clone());
    simulation_one_thresholded_scheduler(config_with_infra);
    simulation_migration_simple(config);
    simulation_migration_component(config_overcommit);
}
