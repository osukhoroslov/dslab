mod common;
use common::assert_float_eq;

use dslab_faas::invocation::{Invocation, InvocationStatus};
use dslab_faas::stats::InvocationStats;

#[test]
fn test_invocation_stats() {
    let mut stats: InvocationStats = Default::default();
    let inv1 = Invocation {
        id: 0,
        func_id: 0,
        duration: 1.0,
        arrival_time: 0.0,
        status: InvocationStatus::Finished,
        host_id: Some(0),
        container_id: Some(0),
        start_time: Some(0.5),
        end_time: Some(2.0),
    };
    let inv2 = Invocation {
        id: 1,
        func_id: 0,
        duration: 1.2,
        arrival_time: 0.0,
        status: InvocationStatus::Finished,
        host_id: Some(0),
        container_id: Some(1),
        start_time: Some(0.5),
        end_time: Some(2.0),
    };
    let inv3 = Invocation {
        id: 2,
        func_id: 0,
        duration: 1.0,
        arrival_time: 2.0,
        status: InvocationStatus::Finished,
        host_id: Some(0),
        container_id: Some(0),
        start_time: Some(2.0),
        end_time: Some(3.0),
    };
    stats.update(&inv1);
    stats.update(&inv2);
    stats.update(&inv3);
    assert_float_eq(stats.abs_total_slowdown.mean(), 1.8 / 3.0, 1e-9);
    assert_float_eq(stats.rel_total_slowdown.mean(), (1.0 + 0.8 / 1.2) / 3.0, 1e-9);
    assert_float_eq(stats.abs_exec_slowdown.mean(), 0.8 / 3.0, 1e-9);
    assert_float_eq(stats.rel_exec_slowdown.mean(), (0.5 + 0.3 / 1.2) / 3.0, 1e-9);
}
