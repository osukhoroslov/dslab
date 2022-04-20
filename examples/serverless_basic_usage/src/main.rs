use serverless::function::Application;
use serverless::invocation::InvocationRequest;
use serverless::resource::{ResourceConsumer, ResourceProvider};
use serverless::simulation::ServerlessSimulation;
use simcore::simulation::Simulation;

fn main() {
    let sim = Simulation::new(1);
    let mut serverless = ServerlessSimulation::new(sim, None, None, None);
    for _ in 0..2 {
        let mem = serverless.create_resource("mem", 2);
        serverless.new_invoker(None, ResourceProvider::new(vec![mem]));
    }
    let fast_mem = serverless.create_resource_requirement("mem", 1);
    let fast = serverless.add_app_with_single_function(Application::new(1, 1., ResourceConsumer::new(vec![fast_mem])));
    let slow_mem = serverless.create_resource_requirement("mem", 2);
    let slow = serverless.add_app_with_single_function(Application::new(1, 2., ResourceConsumer::new(vec![slow_mem])));
    serverless.send_invocation_request(InvocationRequest {
        id: fast,
        duration: 1.0,
        time: 0.,
    });
    serverless.send_invocation_request(InvocationRequest {
        id: slow,
        duration: 1.0,
        time: 0.,
    });
    serverless.send_invocation_request(InvocationRequest {
        id: slow,
        duration: 1.0,
        time: 3.1,
    });
    serverless.step_until_no_events();
    let stats = serverless.get_stats();
    println!(
        "invocations = {}, cold starts = {}, cold starts time = {}",
        stats.invocations, stats.cold_starts, stats.cold_starts_total_time
    );
    println!("wasted memory time = {}", *stats.wasted_resource_time.get(&0).unwrap());
}
