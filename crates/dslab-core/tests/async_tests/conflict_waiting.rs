use serde::Serialize;

use dslab_core::{async_mode::EventKey, Simulation, SimulationContext};

#[derive(Clone, Serialize)]
struct EventWithKey {
    key: EventKey,
}

#[derive(Clone, Serialize)]
struct AnotherEventWithKey {
    key: EventKey,
}

fn make_simulation() -> (Simulation, SimulationContext, SimulationContext, SimulationContext) {
    let mut sim = Simulation::new(123);
    let ctx1 = sim.create_context("ctx1");
    let ctx2 = sim.create_context("ctx2");
    let ctx3 = sim.create_context("ctx3");
    sim.register_key_getter_for::<EventWithKey>(|e| e.key);
    sim.register_key_getter_for::<AnotherEventWithKey>(|e| e.key);

    (sim, ctx1, ctx2, ctx3)
}

#[test]
fn test_ok_different_keys() {
    let (mut sim, ctx1, ctx2, ctx3) = make_simulation();

    ctx2.emit(EventWithKey { key: 1 }, ctx1.id(), 1.);
    ctx3.emit(EventWithKey { key: 2 }, ctx1.id(), 2.);

    sim.spawn(async move {
        futures::join!(
            async {
                let e = ctx1.recv_event_by_key::<EventWithKey>(1).await;
                assert_eq!(ctx1.time(), 1.);
                assert_eq!(e.src, ctx2.id());
            },
            async {
                let e = ctx1.recv_event_by_key::<EventWithKey>(2).await;
                assert_eq!(ctx1.time(), 2.);
                assert_eq!(e.src, ctx3.id());
            }
        );
    });

    sim.step_until_no_events();
    assert_eq!(sim.event_count(), 2);
    assert_eq!(sim.time(), 2.);
}

#[test]
fn test_ok_with_different_sources() {
    let (mut sim, ctx1, ctx2, ctx3) = make_simulation();

    ctx2.emit(EventWithKey { key: 1 }, ctx1.id(), 1.);
    ctx3.emit(EventWithKey { key: 1 }, ctx1.id(), 2.);

    sim.spawn(async move {
        futures::join!(
            async {
                let e = ctx1.recv_event_by_key_from::<EventWithKey>(ctx2.id(), 1).await;
                assert_eq!(ctx1.time(), 1.);
                assert_eq!(e.src, ctx2.id());
            },
            async {
                let e = ctx1.recv_event_by_key_from::<EventWithKey>(ctx3.id(), 1).await;
                assert_eq!(ctx1.time(), 2.);
                assert_eq!(e.src, ctx3.id());
            }
        );
    });

    sim.step_until_no_events();
    assert_eq!(sim.event_count(), 2);
    assert_eq!(sim.time(), 2.);
}

#[test]
#[should_panic(expected = "Failed to create EventFuture")]
fn test_panic_with_different_sources() {
    let (mut sim, ctx1, ctx2, ctx3) = make_simulation();

    ctx2.emit(EventWithKey { key: 1 }, ctx1.id(), 1.);
    ctx3.emit(EventWithKey { key: 1 }, ctx1.id(), 2.);

    sim.spawn(async move {
        futures::join!(
            async {
                let e = ctx1.recv_event_by_key::<EventWithKey>(1).await;
                assert_eq!(ctx1.time(), 1.);
                assert_eq!(e.src, ctx2.id());
            },
            async {
                let e = ctx1.recv_event_by_key::<EventWithKey>(1).await;
                assert_eq!(ctx1.time(), 2.);
                assert_eq!(e.src, ctx3.id());
            }
        );
    });

    sim.step_until_no_events();
    assert_eq!(sim.event_count(), 2);
    assert_eq!(sim.time(), 2.);
}

fn test_panic_with_and_without_sources(first_wait_time: f64, second_wait_time: f64) {
    let (mut sim, ctx1, ctx2, ctx3) = make_simulation();

    ctx2.emit(EventWithKey { key: 1 }, ctx1.id(), 100.);
    ctx3.emit(EventWithKey { key: 1 }, ctx1.id(), 200.);

    sim.spawn(async move {
        futures::join!(
            async {
                ctx1.sleep(first_wait_time).await;
                let e = ctx1.recv_event_by_key_from::<EventWithKey>(ctx2.id(), 1).await;
                assert_eq!(ctx1.time(), 100.);
                assert_eq!(e.src, ctx2.id());
            },
            async {
                ctx1.sleep(second_wait_time).await;
                let e = ctx1.recv_event_by_key::<EventWithKey>(1).await;
                assert_eq!(ctx1.time(), 200.);
                assert_eq!(e.src, ctx3.id());
            }
        );
    });

    sim.step_until_no_events();
    assert_eq!(sim.event_count(), 2);
    assert_eq!(sim.time(), 2.);
}

#[test]
#[should_panic(expected = "Failed to create EventFuture")]
fn test_panic_with_and_without_sources_1() {
    test_panic_with_and_without_sources(10., 20.);
}

#[test]
#[should_panic(expected = "Failed to create EventFuture")]
fn test_panic_with_and_without_sources_2() {
    test_panic_with_and_without_sources(20., 10.);
}

#[test]
fn test_ok_with_different_events() {
    let (mut sim, ctx1, ctx2, ctx3) = make_simulation();

    ctx2.emit(EventWithKey { key: 1 }, ctx1.id(), 1.);
    ctx3.emit(AnotherEventWithKey { key: 1 }, ctx1.id(), 2.);

    sim.spawn(async move {
        futures::join!(
            async {
                let e = ctx1.recv_event_by_key::<EventWithKey>(1).await;
                assert_eq!(ctx1.time(), 1.);
                assert_eq!(e.src, ctx2.id());
            },
            async {
                let e = ctx1.recv_event_by_key::<AnotherEventWithKey>(1).await;
                assert_eq!(ctx1.time(), 2.);
                assert_eq!(e.src, ctx3.id());
            }
        );
    });

    sim.step_until_no_events();
    assert_eq!(sim.event_count(), 2);
    assert_eq!(sim.time(), 2.);
}
