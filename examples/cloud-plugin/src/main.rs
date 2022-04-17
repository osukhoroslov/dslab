use log::info;

use cloud_plugin::config::SimulationConfig;
use cloud_plugin::load_model::ConstLoadModel;
use cloud_plugin::simulation::CloudSimulation;
use cloud_plugin::vm_placement_algorithm::BestFit;
use cloud_plugin::vm_placement_algorithm::BestFitThreshold;
use simcore::simulation::Simulation;

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
    for i in 0..5 {
        cloud_sim.spawn_vm_now(
            i,
            10,
            10,
            2.0,
            Box::new(ConstLoadModel::new(1.0)),
            Box::new(ConstLoadModel::new(1.0)),
            s1,
        );
    }
    // spawn vm_5 - vm_9 on scheduler #2
    for i in 5..10 {
        cloud_sim.spawn_vm_now(
            i,
            10,
            10,
            2.0,
            Box::new(ConstLoadModel::new(1.0)),
            Box::new(ConstLoadModel::new(1.0)),
            s2,
        );
    }

    // spawn vm_10 - vm_14 on scheduler #1
    for i in 10..15 {
        cloud_sim.spawn_vm_now(
            i,
            10,
            10,
            2.0,
            Box::new(ConstLoadModel::new(1.0)),
            Box::new(ConstLoadModel::new(1.0)),
            s1,
        );
    }
    // spawn vm_15 - vm_19 on scheduler #2
    for i in 15..20 {
        cloud_sim.spawn_vm_now(
            i,
            10,
            10,
            2.0,
            Box::new(ConstLoadModel::new(1.0)),
            Box::new(ConstLoadModel::new(1.0)),
            s2,
        );
    }

    cloud_sim.steps(500);

    let end_time = cloud_sim.current_time();
    info!(
        "Total energy consumed on host one: {} watt",
        cloud_sim.host(h1).borrow_mut().get_total_consumed(end_time)
    );
    info!(
        "Total energy consumed on host two: {} watt",
        cloud_sim.host(h2).borrow_mut().get_total_consumed(end_time)
    );
}

fn simulation_one_thresholded_scheduler(sim_config: SimulationConfig) {
    let sim = Simulation::new(123);
    let mut cloud_sim = CloudSimulation::new(sim, sim_config);

    let h1 = cloud_sim.add_host("h1", 30, 30);
    let h2 = cloud_sim.add_host("h2", 30, 30);
    let scheduler_id = cloud_sim.add_scheduler("s", Box::new(BestFitThreshold::new(0.8)));

    for i in 0..10 {
        cloud_sim.spawn_vm_with_delay(
            i,
            10,
            10,
            2.0,
            Box::new(ConstLoadModel::new(0.5)),
            Box::new(ConstLoadModel::new(0.5)),
            scheduler_id,
            i as f64,
        );
    }

    cloud_sim.steps(300);

    let end_time = cloud_sim.current_time();
    info!(
        "Total energy consumed on host one: {} watt",
        cloud_sim.host(h1).borrow_mut().get_total_consumed(end_time)
    );
    info!(
        "Total energy consumed on host two: {} watt",
        cloud_sim.host(h2).borrow_mut().get_total_consumed(end_time)
    );
}

fn simulation_migration_simple(sim_config: SimulationConfig) {
    let sim = Simulation::new(123);
    let mut cloud_sim = CloudSimulation::new(sim, sim_config);

    let h1 = cloud_sim.add_host("h1", 30, 30);
    let h2 = cloud_sim.add_host("h2", 30, 30);
    let scheduler_id = cloud_sim.add_scheduler("s", Box::new(BestFit::new()));

    cloud_sim.spawn_vm_now(
        0,
        10,
        10,
        20.0,
        Box::new(ConstLoadModel::new(0.5)),
        Box::new(ConstLoadModel::new(0.5)),
        scheduler_id,
    );

    cloud_sim.sleep_for(10.);
    cloud_sim.migrate_vm_to_host(0, h2);

    cloud_sim.steps(300);

    let end_time = cloud_sim.current_time();
    info!(
        "Total energy consumed on host one: {} watt",
        cloud_sim.host(h1).borrow_mut().get_total_consumed(end_time)
    );
    info!(
        "Total energy consumed on host two: {} watt",
        cloud_sim.host(h2).borrow_mut().get_total_consumed(end_time)
    );
}

fn main() {
    init_logger();
    let config = SimulationConfig::from_file("config.yaml");
    simulation_two_best_fit_schedulers(config.clone());
    simulation_one_thresholded_scheduler(config.clone());
    simulation_migration_simple(config);
}
