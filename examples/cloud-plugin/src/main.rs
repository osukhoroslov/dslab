use log::info;

use cloud_plugin::load_model::DefaultLoadModel;
use cloud_plugin::simulation::CloudSimulation;
use cloud_plugin::vm_placement_algorithm::BestFitVMPlacement;
use core::sim::Simulation;

extern crate env_logger;

fn main() {
    env_logger::init();

    let sim = Simulation::new(123);
    let mut cloud_sim = CloudSimulation::new(sim);

    cloud_sim.add_host("h1", 30, 30);
    cloud_sim.add_host("h2", 30, 30);
    cloud_sim.add_scheduler("s1", Box::new(BestFitVMPlacement::new()));
    cloud_sim.add_scheduler("s2", Box::new(BestFitVMPlacement::new()));

    // spawn vm_0 - vm_4 on scheduler #1
    for i in 0..5 {
        let vm_name = format!("v{}", i);
        let _vm = cloud_sim.spawn_vm(
            &vm_name,
            10,
            10,
            2.0,
            "s1",
            Box::new(DefaultLoadModel::new()),
            Box::new(DefaultLoadModel::new()),
        );
    }
    // spawn vm_5 - vm_9 on scheduler #2
    for i in 5..10 {
        let vm_name = format!("v{}", i);
        let _vm = cloud_sim.spawn_vm(
            &vm_name,
            10,
            10,
            2.0,
            "s2",
            Box::new(DefaultLoadModel::new()),
            Box::new(DefaultLoadModel::new()),
        );
    }

    cloud_sim.steps(150);
    // spawn vm_10 - vm_14 on scheduler #1
    for i in 10..15 {
        let vm_name = format!("v{}", i);
        let _vm = cloud_sim.spawn_vm(
            &vm_name,
            10,
            10,
            2.0,
            "s1",
            Box::new(DefaultLoadModel::new()),
            Box::new(DefaultLoadModel::new()),
        );
    }
    // spawn vm_15 - vm_19 on scheduler #2
    for i in 15..20 {
        let vm_name = format!("v{}", i);
        let _vm = cloud_sim.spawn_vm(
            &vm_name,
            10,
            10,
            2.0,
            "s2",
            Box::new(DefaultLoadModel::new()),
            Box::new(DefaultLoadModel::new()),
        );
    }

    cloud_sim.steps(380);

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
