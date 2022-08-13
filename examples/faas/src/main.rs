use std::io::Write;

use env_logger::Builder;

use dslab_core::simulation::Simulation;
use dslab_faas::function::Application;
use dslab_faas::resource::{ResourceConsumer, ResourceProvider};
use dslab_faas::simulation::ServerlessSimulation;

fn main() {
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();
    let mut sim = ServerlessSimulation::new(Simulation::new(1), Default::default());
    for _ in 0..2 {
        let mem = sim.create_resource("mem", 2);
        sim.add_host(None, ResourceProvider::new(vec![mem]), 4);
    }
    let fast_mem = sim.create_resource_requirement("mem", 1);
    let fast = sim.add_app_with_single_function(Application::new(1, 1., 1., ResourceConsumer::new(vec![fast_mem])));
    let slow_mem = sim.create_resource_requirement("mem", 2);
    let slow = sim.add_app_with_single_function(Application::new(1, 2., 1., ResourceConsumer::new(vec![slow_mem])));
    sim.send_invocation_request(fast, 1.0, 0.0);
    sim.send_invocation_request(slow, 1.0, 0.0);
    sim.send_invocation_request(slow, 1.0, 3.1);
    sim.step_until_no_events();
    let stats = sim.get_stats();
    println!(
        "invocations = {}, cold starts = {}, mean cold start latency = {}",
        stats.invocations,
        stats.cold_starts,
        stats.cold_start_latency.mean()
    );
    println!(
        "wasted memory time = {}",
        stats.wasted_resource_time.get(&0).unwrap().sum()
    );
    println!("mean abs slowdown = {}", stats.abs_slowdown.mean());
}
