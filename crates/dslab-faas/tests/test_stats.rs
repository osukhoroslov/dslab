mod common;
use common::assert_float_eq;

use dslab_faas::invocation::{Invocation, InvocationRequest};
use dslab_faas::stats::InvocationStats;

#[test]
fn test_invocation_stats() {
    let mut stats: InvocationStats = Default::default();
    let req1 = InvocationRequest {
        func_id: 0,
        duration: 1.0,
        time: 0.0,
        id: 0,
    };
    let inv1 = Invocation {
        id: 0,
        request: req1,
        host_id: 0,
        container_id: 0,
        started: 0.5,
        finished: Some(2.0),
    };
    let req2 = InvocationRequest {
        func_id: 0,
        duration: 1.2,
        time: 0.0,
        id: 1,
    };
    let inv2 = Invocation {
        id: 1,
        request: req2,
        host_id: 0,
        container_id: 1,
        started: 0.5,
        finished: Some(2.0),
    };
    let req3 = InvocationRequest {
        func_id: 0,
        duration: 1.0,
        time: 2.0,
        id: 2,
    };
    let inv3 = Invocation {
        id: 2,
        request: req3,
        host_id: 0,
        container_id: 0,
        started: 2.0,
        finished: Some(3.0),
    };
    stats.update(&inv1);
    stats.update(&inv2);
    stats.update(&inv3);
    assert_float_eq(stats.abs_total_slowdown.mean(), 1.8 / 3.0, 1e-9);
    assert_float_eq(stats.rel_total_slowdown.mean(), (1.0 + 0.8 / 1.2) / 3.0, 1e-9);
    assert_float_eq(stats.abs_exec_slowdown.mean(), 0.8 / 3.0, 1e-9);
    assert_float_eq(stats.rel_exec_slowdown.mean(), (0.5 + 0.3 / 1.2) / 3.0, 1e-9);
}
