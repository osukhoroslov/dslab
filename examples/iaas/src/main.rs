use log::info;

use clap::Parser;

use dslab_core::simulation::Simulation;
use dslab_iaas::core::config::SimulationConfig;
use dslab_iaas::core::load_model::ConstantLoadModel;
use dslab_iaas::core::load_model::LoadModel;
use dslab_iaas::core::vm_placement_algorithms::best_fit::BestFit;
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

/// Execution of 20 VMs with constant 100% load on two hosts using two BestFit schedulers.
fn example_two_schedulers() {
    let sim = Simulation::new(123);
    let config = SimulationConfig::from_file("config.yaml");
    let mut cloud_sim = CloudSimulation::new(sim, config);

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

    cloud_sim.step_for_duration(20.);

    let end_time = cloud_sim.current_time();
    info!(
        "Total energy consumed by host one: {:.2}",
        cloud_sim.host(h1).borrow_mut().get_energy_consumed(end_time)
    );
    info!(
        "Total energy consumed by host two: {:.2}",
        cloud_sim.host(h2).borrow_mut().get_energy_consumed(end_time)
    );
}

/// Execution of 10 VMs with constant 50% load (taken from workload.json) on two hosts
/// using a single BestFitThreshold scheduler with resource overcommitment.
/// The hosts and scheduler are initialized directly from the config file.
fn example_single_scheduler_overcommit() {
    let sim = Simulation::new(123);
    let config = SimulationConfig::from_file("config_with_infrastructure.yaml");
    let mut cloud_sim = CloudSimulation::new(sim, config);
    let scheduler_id = cloud_sim.lookup_id("s");

    let mut dataset = StandardDatasetReader::new();
    dataset.parse("workload.json");
    cloud_sim.spawn_vms_from_dataset(scheduler_id, &mut dataset);

    cloud_sim.step_for_duration(20.);

    let end_time = cloud_sim.current_time();
    info!(
        "Total energy consumed by host one: {:.2}",
        cloud_sim.host_by_name("h1").borrow_mut().get_energy_consumed(end_time)
    );
    info!(
        "Total energy consumed by host two: {:.2}",
        cloud_sim.host_by_name("h2").borrow_mut().get_energy_consumed(end_time)
    );
}

/// Manual migration of a single VM between two hosts.
fn example_manual_migration() {
    let sim = Simulation::new(123);
    let config = SimulationConfig::from_file("config.yaml");
    let mut cloud_sim = CloudSimulation::new(sim, config);

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
    cloud_sim.step_for_duration(12.);

    let end_time = cloud_sim.current_time();
    info!(
        "Total energy consumed by host one: {:.2}",
        cloud_sim.host(h1).borrow_mut().get_energy_consumed(end_time)
    );
    info!(
        "Total energy consumed by host two: {:.2}",
        cloud_sim.host(h2).borrow_mut().get_energy_consumed(end_time)
    );
}

#[derive(Clone)]
pub struct DecreaseLoadModel {}

impl DecreaseLoadModel {
    pub fn new() -> Self {
        Self {}
    }
}

impl LoadModel for DecreaseLoadModel {
    fn get_resource_load(&self, time: f64, _time_from_start: f64) -> f64 {
        // linear drop from 100% to zero within the first 500 time points
        // then linear growth back from zero to 100% during the next 500 time points
        return if time <= 500. {
            let linear = (500. - time) / 500.;
            linear.max(0.)
        } else {
            let linear = (time - 500.) / 500.;
            linear.min(1.)
        };
    }
}

/// Execution of 10 VMs with custom load function (see above) on 10 hosts with overcommitment
/// using a single BestFit scheduler and VM migrator performing periodical VM consolidation.
fn example_vm_migrator() {
    let sim = Simulation::new(123);
    let config = SimulationConfig::from_file("config_overcommit.yaml");
    let mut cloud_sim = CloudSimulation::new(sim, config);

    for i in 0..10 {
        cloud_sim.add_host(&format!("h{}", i), 50, 50);
    }
    let scheduler_id = cloud_sim.add_scheduler("s", Box::new(BestFit::new()));

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

/// DSLab IaaS Examples
#[derive(Parser, Debug)]
#[clap(about, long_about = None)]
struct Args {
    #[clap(long, short)]
    example: Option<String>,
}

fn main() {
    init_logger();
    let args = Args::parse();
    let example = args.example.as_ref();
    if example.is_none() || example.unwrap() == "two-schedulers" {
        println!("\n--- TWO SCHEDULERS ---\n");
        example_two_schedulers();
    }
    if example.is_none() || example.unwrap() == "single-scheduler-overcommit" {
        println!("\n--- SINGLE SCHEDULER WITH OVERCOMMIT ---\n");
        example_single_scheduler_overcommit();
    }
    if example.is_none() || example.unwrap() == "manual-migration" {
        println!("\n--- MANUAL MIGRATION ---\n");
        example_manual_migration();
    }
    if example.is_none() || example.unwrap() == "vm-migrator" {
        println!("\n--- VM MIGRATOR ---\n");
        example_vm_migrator();
    }
}
