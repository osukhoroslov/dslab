use serverless::function::Group;
use serverless::invocation::InvocationRequest;
use serverless::resource::{Resource, ResourceConsumer, ResourceProvider, ResourceRequirement};
use serverless::simulation::ServerlessSimulation;

use simcore::simulation::Simulation;

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

fn main() {
    let sim = Simulation::new(1);
    let mut serverless = ServerlessSimulation::new(sim, None, None, None);
    for _ in 0..2 {
        serverless.new_invoker(
            None,
            ResourceProvider::new(HashMap::<String, Resource>::from([(
                "mem".to_string(),
                Resource::new("mem".to_string(), 2),
            )])),
        );
    }
    let fast = serverless.new_function_with_group(Group::new(
        1,
        1.,
        ResourceConsumer::new(HashMap::<String, ResourceRequirement>::from([(
            "mem".to_string(),
            ResourceRequirement::new("mem".to_string(), 1),
        )])),
    ));
    let slow = serverless.new_function_with_group(Group::new(
        1,
        2.,
        ResourceConsumer::new(HashMap::<String, ResourceRequirement>::from([(
            "mem".to_string(),
            ResourceRequirement::new("mem".to_string(), 2),
        )])),
    ));
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
    println!(
        "wasted memory time = {}",
        *stats.wasted_resource_time.get("mem").unwrap()
    );
}
