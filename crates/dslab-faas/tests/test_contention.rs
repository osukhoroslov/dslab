use dslab_core::simulation::Simulation;
use dslab_faas::function::Application;
use dslab_faas::resource::{ResourceConsumer, ResourceProvider};
use dslab_faas::simulation::ServerlessSimulation;

fn assert_float_eq(x: f64, y: f64, eps: f64) {
    assert!(x > y - eps && x < y + eps);
}

#[test]
fn test_different_shares() {
    let sim = Simulation::new(1);
    let mut serverless = ServerlessSimulation::new(sim, Default::default());
    let mem = serverless.create_resource("mem", 100);
    serverless.add_host(None, ResourceProvider::new(vec![mem]), 7);
    let mem2 = serverless.create_resource_requirement("mem", 1);
    let func1 =
        serverless.add_app_with_single_function(Application::new(1, 0., 2., ResourceConsumer::new(vec![mem2.clone()])));
    let func2 = serverless.add_app_with_single_function(Application::new(1, 0., 3., ResourceConsumer::new(vec![mem2])));
    for _ in 0..5 {
        serverless.send_invocation_request(func1, 1.0, 0.0);
        serverless.send_invocation_request(func2, 2.0, 0.0);
    }
    serverless.step_until_no_events();
    let stats = serverless.get_stats();
    let abs_slowdown = stats.abs_slowdown.mean();
    let rel_slowdown = stats.rel_slowdown.mean();
    assert_float_eq(abs_slowdown, 3.1428571428571432, 1e-9);
    assert_float_eq(rel_slowdown, 2.2142857142857144, 1e-9);
}

#[test]
fn test_equal_shares() {
    let sim = Simulation::new(1);
    let mut serverless = ServerlessSimulation::new(sim, Default::default());
    let mem = serverless.create_resource("mem", 100);
    serverless.add_host(None, ResourceProvider::new(vec![mem]), 4);
    let mem2 = serverless.create_resource_requirement("mem", 1);
    let func = serverless.add_app_with_single_function(Application::new(1, 0., 1., ResourceConsumer::new(vec![mem2])));
    for _ in 0..10 {
        serverless.send_invocation_request(func, 1.0, 0.0);
    }
    serverless.step_until_no_events();
    let stats = serverless.get_stats();
    let abs_slowdown = stats.abs_slowdown.mean();
    let rel_slowdown = stats.rel_slowdown.mean();
    assert_float_eq(abs_slowdown, 1.5, 1e-9);
    assert_float_eq(rel_slowdown, 1.5, 1e-9);
}

#[test]
fn test_no_contention() {
    let sim = Simulation::new(1);
    let mut serverless = ServerlessSimulation::new(sim, Default::default());
    let mem = serverless.create_resource("mem", 100);
    serverless.add_host(None, ResourceProvider::new(vec![mem]), 10);
    let mem2 = serverless.create_resource_requirement("mem", 1);
    let func = serverless.add_app_with_single_function(Application::new(1, 0., 2., ResourceConsumer::new(vec![mem2])));
    for _ in 0..5 {
        serverless.send_invocation_request(func, 1.0, 0.0);
    }
    serverless.step_until_no_events();
    let stats = serverless.get_stats();
    let abs_slowdown = stats.abs_slowdown.mean();
    let rel_slowdown = stats.rel_slowdown.mean();
    assert_float_eq(abs_slowdown, 0., 1e-9);
    assert_float_eq(rel_slowdown, 0., 1e-9);
}
