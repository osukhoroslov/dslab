use futures::{select, FutureExt};
use serde::Serialize;

use dslab_core::Simulation;

#[derive(Clone, Serialize)]
struct TestEvent {
    key: u64,
}

#[test]
fn test_select_timeout() {
    let mut sim = Simulation::new(123);
    let ctx = sim.create_context("comp");
    sim.register_key_getter_for::<TestEvent>(|e| e.key);

    sim.spawn(async move {
        {
            select! {
                _ = ctx.recv_event_by_key::<TestEvent>(1).with_timeout(10.).fuse() => {
                    println!("received 1")
                }
                _ = ctx.recv_event_by_key::<TestEvent>(2).with_timeout(11.).fuse() => {
                    println!("received 2")
                }
            }
        }
    });

    sim.step_until_no_events();
    assert_eq!(sim.time(), 10.);
}

#[test]
fn test_select_completed() {
    let mut sim = Simulation::new(123);
    let comp_ctx = sim.create_context("comp");
    let comp_id = comp_ctx.id();
    sim.register_key_getter_for::<TestEvent>(|e| e.key);

    sim.spawn(async move {
        {
            select! {
                _ = comp_ctx.recv_event_by_key::<TestEvent>(1).with_timeout(10.).fuse() => {
                    println!("received 1")
                }
                _ = comp_ctx.recv_event_by_key::<TestEvent>(2).with_timeout(11.).fuse() => {
                    println!("received 2")
                }
            }
        }
    });

    let root_ctx = sim.create_context("root");
    root_ctx.emit(TestEvent { key: 1 }, comp_id, 5.);

    sim.step_until_no_events();
    assert_eq!(sim.time(), 5.);
}
