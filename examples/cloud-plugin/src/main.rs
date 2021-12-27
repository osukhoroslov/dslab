use log::info;

use core::sim::Simulation;
use cloud_plugin::simulation::CloudSimulation;

extern crate env_logger;

fn main() {
    env_logger::init();

    let sim = Simulation::new(123);
    let mut cloud_sim = CloudSimulation::new(sim);
    cloud_sim.init_actors();

    let host_one = cloud_sim.spawn_host("h1", 30, 30);
    let host_two = cloud_sim.spawn_host("h2", 30, 30);
    let allocator = cloud_sim.spawn_allocator("a");

    for i in 0..10 {
        let vm_name = format!("v{}", i);
        let _vm = cloud_sim.spawn_vm(&vm_name, 10, 10, 2.0, allocator.clone());
    }

    cloud_sim.steps(170);
    info!(
        "Total energy consumed on host one: {} watt",
        host_one.borrow_mut().get_total_consumed(cloud_sim.current_time())
    );
    info!(
        "Total energy consumed on host two: {} watt",
        host_two.borrow_mut().get_total_consumed(cloud_sim.current_time())
    );
}
