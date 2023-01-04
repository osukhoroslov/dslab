mod common;
use common::assert_float_eq;

use std::boxed::Box;

use dslab_core::simulation::Simulation;
use dslab_faas::coldstart::FixedTimeColdStartPolicy;
use dslab_faas::config::Config;
use dslab_faas::function::Application;
use dslab_faas::resource::{ResourceConsumer, ResourceProvider};
use dslab_faas::simulation::ServerlessSimulation;

#[test]
fn test_simulation() {
    let config = Config {
        coldstart_policy: Box::new(FixedTimeColdStartPolicy::new(1.0, 0.0)),
        ..Default::default()
    };
    let mut sim = ServerlessSimulation::new(Simulation::new(1), config);
    {
        let mem = sim.create_resource("mem", 2);
        sim.add_host(None, ResourceProvider::new(vec![mem]), 1);
    }
    let mem1 = sim.create_resource_requirement("mem", 1);
    let f1 = sim.add_app_with_single_function(Application::new(1, 1., 1., ResourceConsumer::new(vec![mem1])));
    let mem2 = sim.create_resource_requirement("mem", 1);
    let f2 = sim.add_app_with_single_function(Application::new(1, 1., 1., ResourceConsumer::new(vec![mem2])));
    sim.send_invocation_request(f1, 1.0, 0.0);
    sim.send_invocation_request(f2, 1.0, 0.0);
    sim.send_invocation_request(f1, 1.0, 4.0 - 1e-9);
    sim.step_until_no_events();
    let monitor = sim.stats();
    let stats = monitor.global_stats.invocation_stats;
    assert_eq!(stats.invocations, 3);
    assert_eq!(stats.cold_starts, 2);
    assert_float_eq(stats.cold_start_latency.min().unwrap(), 1.0, 1e-9);
    assert_float_eq(stats.cold_start_latency.max().unwrap(), 1.0, 1e-9);
    assert_float_eq(stats.abs_exec_slowdown.mean(), 2.0 / 3.0, 1e-9);
    assert_float_eq(stats.rel_exec_slowdown.mean(), 2.0 / 3.0, 1e-9);
    assert_float_eq(stats.abs_total_slowdown.mean(), 4.0 / 3.0, 1e-9);
    assert_float_eq(stats.rel_total_slowdown.mean(), 4.0 / 3.0, 1e-9);
    assert_float_eq(monitor.global_stats.wasted_resource_time[&0].sum(), 3.0 - 1e-9, 1e-9);
    let stats1 = &monitor.func_stats[&0];
    assert_float_eq(stats1.abs_total_slowdown.mean(), 1.0, 1e-9);
    let stats2 = &monitor.func_stats[&1];
    assert_float_eq(stats2.abs_total_slowdown.mean(), 2.0, 1e-9);
}

#[test]
fn test_simulation_with_invoker_queueing() {
    let config = Config {
        coldstart_policy: Box::new(FixedTimeColdStartPolicy::new(1.0, 0.0)),
        ..Default::default()
    };
    let mut sim = ServerlessSimulation::new(Simulation::new(1), config);
    {
        let mem = sim.create_resource("mem", 2);
        sim.add_host(None, ResourceProvider::new(vec![mem]), 1);
    }
    let mem1 = sim.create_resource_requirement("mem", 1);
    let f1 = sim.add_app_with_single_function(Application::new(1, 1., 1., ResourceConsumer::new(vec![mem1])));
    let mem2 = sim.create_resource_requirement("mem", 1);
    let f2 = sim.add_app_with_single_function(Application::new(1, 1., 1., ResourceConsumer::new(vec![mem2])));
    sim.send_invocation_request(f1, 1.0, 0.0);
    sim.send_invocation_request(f2, 1.0, 0.0);
    sim.send_invocation_request(f1, 1.0, 2.9);
    sim.step_until_no_events();
    let monitor = sim.stats();
    let stats = monitor.global_stats.invocation_stats;
    assert_eq!(stats.invocations, 3);
    assert_eq!(stats.cold_starts, 2);
    assert_float_eq(stats.cold_start_latency.min().unwrap(), 1.0, 1e-9);
    assert_float_eq(stats.cold_start_latency.max().unwrap(), 1.0, 1e-9);
    assert_float_eq(stats.abs_exec_slowdown.mean(), 2.0 / 3.0, 1e-9);
    assert_float_eq(stats.rel_exec_slowdown.mean(), 2.0 / 3.0, 1e-9);
    assert_float_eq(stats.abs_total_slowdown.mean(), 4.1 / 3.0, 1e-9);
    assert_float_eq(stats.rel_total_slowdown.mean(), 4.1 / 3.0, 1e-9);
    assert_float_eq(stats.queueing_time.mean(), 0.1, 1e-9);
    assert_float_eq(
        stats.queueing_time.extend(stats.invocations as usize).mean(),
        0.1 / 3.0,
        1e-9,
    );
    assert_float_eq(monitor.global_stats.wasted_resource_time[&0].sum(), 2.0, 1e-9);
    let stats1 = &monitor.func_stats[&0];
    assert_float_eq(stats1.abs_total_slowdown.mean(), 1.05, 1e-9);
    assert_float_eq(stats1.queueing_time.mean(), 0.1, 1e-9);
    assert_float_eq(
        stats1.queueing_time.extend(stats1.invocations as usize).mean(),
        0.05,
        1e-9,
    );
    let stats2 = &monitor.func_stats[&1];
    assert_float_eq(stats2.abs_total_slowdown.mean(), 2.0, 1e-9);
    assert!(stats2.queueing_time.is_empty());
}
