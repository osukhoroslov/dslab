mod common;
use common::assert_float_eq;

use dslab_core::simulation::Simulation;
use dslab_faas::function::Application;
use dslab_faas::resource::{ResourceConsumer, ResourceProvider};
use dslab_faas::simulation::ServerlessSimulation;

#[test]
fn test_concurrency() {
    let sim = Simulation::new(1);
    let mut serverless = ServerlessSimulation::new(sim, Default::default());
    let mem = serverless.create_resource("mem", 2);
    serverless.add_host(None, ResourceProvider::new(vec![mem]), 2);
    let mem2 = serverless.create_resource_requirement("mem", 1);
    let func1 =
        serverless.add_app_with_single_function(Application::new(2, 0., 1., ResourceConsumer::new(vec![mem2.clone()])));
    let func2 = serverless.add_app_with_single_function(Application::new(3, 0., 2., ResourceConsumer::new(vec![mem2])));
    let mut func1_invocations = Vec::with_capacity(2);
    let mut func2_invocations = Vec::with_capacity(3);
    for _ in 0..2 {
        func1_invocations.push(serverless.send_invocation_request(func1, 1.0, 0.0));
    }
    for _ in 0..3 {
        func2_invocations.push(serverless.send_invocation_request(func2, 1.0, 0.0));
    }
    serverless.step_until_no_events();
    for id in func1_invocations.drain(..) {
        let invocation = serverless.get_invocation(id).unwrap();
        assert_float_eq(invocation.finished.unwrap(), 3., 1e-9);
    }
    for id in func2_invocations.drain(..) {
        let invocation = serverless.get_invocation(id).unwrap();
        assert_float_eq(invocation.finished.unwrap(), 4., 1e-9);
    }
    let stats = serverless.get_global_stats().invocation_stats;
    let abs_exec_slowdown = stats.abs_exec_slowdown.mean();
    let rel_exec_slowdown = stats.rel_exec_slowdown.mean();
    assert_float_eq(abs_exec_slowdown, 2.6, 1e-9);
    assert_float_eq(rel_exec_slowdown, 2.6, 1e-9);
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
    let mut short_invocations = Vec::with_capacity(5);
    let mut long_invocations = Vec::with_capacity(5);
    for _ in 0..5 {
        short_invocations.push(serverless.send_invocation_request(func1, 1.0, 0.0));
        long_invocations.push(serverless.send_invocation_request(func2, 2.0, 0.0));
    }
    serverless.step_until_no_events();
    for id in short_invocations.drain(..) {
        let invocation = serverless.get_invocation(id).unwrap();
        assert_float_eq(invocation.finished.unwrap(), 3.5714285714285716, 1e-9);
    }
    for id in long_invocations.drain(..) {
        let invocation = serverless.get_invocation(id).unwrap();
        assert_float_eq(invocation.finished.unwrap(), 5.714285714285714, 1e-9);
    }
    let stats = serverless.get_global_stats().invocation_stats;
    let abs_exec_slowdown = stats.abs_exec_slowdown.mean();
    let rel_exec_slowdown = stats.rel_exec_slowdown.mean();
    assert_float_eq(abs_exec_slowdown, 3.1428571428571432, 1e-9);
    assert_float_eq(rel_exec_slowdown, 2.2142857142857144, 1e-9);
}

#[test]
fn test_different_start_times() {
    let sim = Simulation::new(1);
    let mut serverless = ServerlessSimulation::new(sim, Default::default());
    let mem = serverless.create_resource("mem", 100);
    serverless.add_host(None, ResourceProvider::new(vec![mem]), 7);
    let mem2 = serverless.create_resource_requirement("mem", 1);
    let func1 =
        serverless.add_app_with_single_function(Application::new(1, 0., 2., ResourceConsumer::new(vec![mem2.clone()])));
    let func2 = serverless.add_app_with_single_function(Application::new(1, 0., 3., ResourceConsumer::new(vec![mem2])));
    let mut delayed_invocations = Vec::with_capacity(5);
    let mut initial_invocations = Vec::with_capacity(5);
    for _ in 0..5 {
        delayed_invocations.push(serverless.send_invocation_request(func1, 2.0, 0.5));
        initial_invocations.push(serverless.send_invocation_request(func2, 2.0, 0.0));
    }
    serverless.step_until_no_events();
    for id in delayed_invocations.drain(..) {
        let invocation = serverless.get_invocation(id).unwrap();
        assert_float_eq(invocation.finished.unwrap(), 7.142857142857142, 1e-9);
    }
    for id in initial_invocations.drain(..) {
        let invocation = serverless.get_invocation(id).unwrap();
        assert_float_eq(invocation.finished.unwrap(), 6.809523809523809, 1e-9);
    }
    let stats = serverless.get_global_stats().invocation_stats;
    let abs_exec_slowdown = stats.abs_exec_slowdown.mean();
    let rel_exec_slowdown = stats.rel_exec_slowdown.mean();
    assert_float_eq(abs_exec_slowdown, 4.726190476190476, 1e-9);
    assert_float_eq(rel_exec_slowdown, 2.363095238095238, 1e-9);
}

#[test]
fn test_equal_shares() {
    let sim = Simulation::new(1);
    let mut serverless = ServerlessSimulation::new(sim, Default::default());
    let mem = serverless.create_resource("mem", 100);
    serverless.add_host(None, ResourceProvider::new(vec![mem]), 4);
    let mem2 = serverless.create_resource_requirement("mem", 1);
    let func = serverless.add_app_with_single_function(Application::new(1, 0., 1., ResourceConsumer::new(vec![mem2])));
    let mut invocations = Vec::with_capacity(10);
    for _ in 0..10 {
        invocations.push(serverless.send_invocation_request(func, 1.0, 0.0));
    }
    serverless.step_until_no_events();
    for id in invocations.drain(..) {
        let invocation = serverless.get_invocation(id).unwrap();
        assert_float_eq(invocation.finished.unwrap(), 2.5, 1e-9);
    }
    let stats = serverless.get_global_stats().invocation_stats;
    let abs_exec_slowdown = stats.abs_exec_slowdown.mean();
    let rel_exec_slowdown = stats.rel_exec_slowdown.mean();
    assert_float_eq(abs_exec_slowdown, 1.5, 1e-9);
    assert_float_eq(rel_exec_slowdown, 1.5, 1e-9);
}

#[test]
fn test_no_contention() {
    let sim = Simulation::new(1);
    let mut serverless = ServerlessSimulation::new(sim, Default::default());
    let mem = serverless.create_resource("mem", 100);
    serverless.add_host(None, ResourceProvider::new(vec![mem]), 10);
    let mem2 = serverless.create_resource_requirement("mem", 1);
    let func = serverless.add_app_with_single_function(Application::new(1, 0., 2., ResourceConsumer::new(vec![mem2])));
    let mut invocations = Vec::with_capacity(5);
    for _ in 0..5 {
        invocations.push(serverless.send_invocation_request(func, 1.0, 0.0));
    }
    serverless.step_until_no_events();
    for id in invocations.drain(..) {
        let invocation = serverless.get_invocation(id).unwrap();
        assert_float_eq(invocation.finished.unwrap(), 1., 1e-9);
    }
    let stats = serverless.get_global_stats().invocation_stats;
    let abs_exec_slowdown = stats.abs_exec_slowdown.mean();
    let rel_exec_slowdown = stats.rel_exec_slowdown.mean();
    assert_float_eq(abs_exec_slowdown, 0., 1e-9);
    assert_float_eq(rel_exec_slowdown, 0., 1e-9);
}
