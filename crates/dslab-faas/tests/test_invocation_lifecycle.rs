use std::boxed::Box;

use dslab_core::simulation::Simulation;
use dslab_faas::coldstart::FixedTimeColdStartPolicy;
use dslab_faas::config::Config;
use dslab_faas::function::Application;
use dslab_faas::invocation::InvocationStatus;
use dslab_faas::resource::{ResourceConsumer, ResourceProvider};
use dslab_faas::simulation::ServerlessSimulation;

#[test]
fn test_invocation_lifecycle() {
    let config = Config {
        coldstart_policy: Box::new(FixedTimeColdStartPolicy::new(1.0, 0.0)),
        disable_contention: true,
        ..Default::default()
    };
    let mut sim = ServerlessSimulation::new(Simulation::new(1), config);
    let host_mem = sim.create_resource("mem", 1);
    sim.add_host(None, ResourceProvider::new(vec![host_mem]), 1);
    let fn_mem = sim.create_resource_requirement("mem", 1);
    let f = sim.add_app_with_single_function(Application::new(1, 1., 1., ResourceConsumer::new(vec![fn_mem])));
    sim.send_invocation_request(f, 1.0, 0.0);
    sim.send_invocation_request(f, 1.0, 1.2);
    assert_eq!(sim.get_invocation(0).status, InvocationStatus::NotArrived);
    assert_eq!(sim.get_invocation(1).status, InvocationStatus::NotArrived);
    sim.step();
    assert_eq!(sim.get_invocation(0).status, InvocationStatus::WaitingForContainer);
    assert_eq!(sim.get_invocation(1).status, InvocationStatus::NotArrived);
    sim.step();
    assert_eq!(sim.get_invocation(0).status, InvocationStatus::Running);
    assert_eq!(sim.get_invocation(1).status, InvocationStatus::NotArrived);
    sim.step();
    assert_eq!(sim.get_invocation(0).status, InvocationStatus::Running);
    assert_eq!(sim.get_invocation(1).status, InvocationStatus::Queued);
    sim.step();
    assert_eq!(sim.get_invocation(0).status, InvocationStatus::Finished);
    assert_eq!(sim.get_invocation(1).status, InvocationStatus::Running);
    sim.step_until_no_events();
    assert_eq!(sim.get_invocation(0).status, InvocationStatus::Finished);
    assert_eq!(sim.get_invocation(1).status, InvocationStatus::Finished);
}
