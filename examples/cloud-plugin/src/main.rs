extern crate env_logger;

use std::cell::RefCell;
use std::rc::Rc;
use sugars::{rc, refcell};

use log::info;

use cloud_plugin::config::SimulationConfig;
use cloud_plugin::load_model::ConstLoadModel;
use cloud_plugin::simulation::CloudSimulation;
use cloud_plugin::vm_placement_algorithm::BestFit;
use cloud_plugin::vm_placement_algorithm::BestFitThreshold;
use simcore::simulation::Simulation;

fn simulation_two_best_fit_schedulers(sim_config: Rc<RefCell<SimulationConfig>>) {
    env_logger::init();

    let sim = Simulation::new(123);
    let mut cloud_sim = CloudSimulation::new(sim, sim_config);

    cloud_sim.add_host("h1", 30, 30);
    cloud_sim.add_host("h2", 30, 30);
    cloud_sim.add_scheduler("s1", Box::new(BestFit::new()));
    cloud_sim.add_scheduler("s2", Box::new(BestFit::new()));

    // spawn vm_0 - vm_4 on scheduler #1
    for i in 0..5 {
        let vm_name = format!("v{}", i);
        let _vm = cloud_sim.spawn_vm_now(
            &vm_name,
            10,
            10,
            2.0,
            Box::new(ConstLoadModel::new(1.0)),
            Box::new(ConstLoadModel::new(1.0)),
            "s1",
        );
    }
    // spawn vm_5 - vm_9 on scheduler #2
    for i in 5..10 {
        let vm_name = format!("v{}", i);
        let _vm = cloud_sim.spawn_vm_now(
            &vm_name,
            10,
            10,
            2.0,
            Box::new(ConstLoadModel::new(1.0)),
            Box::new(ConstLoadModel::new(1.0)),
            "s2",
        );
    }

    // spawn vm_10 - vm_14 on scheduler #1
    for i in 10..15 {
        let vm_name = format!("v{}", i);
        let _vm = cloud_sim.spawn_vm_now(
            &vm_name,
            10,
            10,
            2.0,
            Box::new(ConstLoadModel::new(1.0)),
            Box::new(ConstLoadModel::new(1.0)),
            "s1",
        );
    }
    // spawn vm_15 - vm_19 on scheduler #2
    for i in 15..20 {
        let vm_name = format!("v{}", i);
        let _vm = cloud_sim.spawn_vm_now(
            &vm_name,
            10,
            10,
            2.0,
            Box::new(ConstLoadModel::new(1.0)),
            Box::new(ConstLoadModel::new(1.0)),
            "s2",
        );
    }

    cloud_sim.steps(500);

    let end_time = cloud_sim.current_time();
    info!(
        "Total energy consumed on host one: {} watt",
        cloud_sim.host("h1").borrow_mut().get_total_consumed(end_time)
    );
    info!(
        "Total energy consumed on host two: {} watt",
        cloud_sim.host("h2").borrow_mut().get_total_consumed(end_time)
    );
}

fn simulation_one_thresholded_scheduler(sim_config: Rc<RefCell<SimulationConfig>>) {
    let sim = Simulation::new(123);
    let mut cloud_sim = CloudSimulation::new(sim, sim_config);

    cloud_sim.add_host("h1", 30, 30);
    cloud_sim.add_host("h2", 30, 30);
    cloud_sim.add_scheduler("s", Box::new(BestFitThreshold::new(0.8)));

    for i in 0..10 {
        let vm_name = format!("v{}", i);
        let _vm = cloud_sim.spawn_vm_with_delay(
            &vm_name,
            10,
            10,
            2.0,
            Box::new(ConstLoadModel::new(0.5)),
            Box::new(ConstLoadModel::new(0.5)),
            "s",
            i as f64,
        );
    }

    cloud_sim.steps(300);

    let end_time = cloud_sim.current_time();
    info!(
        "Total energy consumed on host one: {} watt",
        cloud_sim.host("h1").borrow_mut().get_total_consumed(end_time)
    );
    info!(
        "Total energy consumed on host two: {} watt",
        cloud_sim.host("h2").borrow_mut().get_total_consumed(end_time)
    );
}

fn main() {
    let config = rc!(refcell!(SimulationConfig::from_file("config.yaml")));
    simulation_two_best_fit_schedulers(config.clone());
    simulation_one_thresholded_scheduler(config.clone());
}
