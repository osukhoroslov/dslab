use dslab_core::async_core::AwaitResult;
use dslab_core::async_core::EventKey;
use dslab_core::Simulation;
use dslab_core::SimulationContext;
use futures::select;
use futures::FutureExt;
use serde::Serialize;

#[derive(Clone, Serialize)]
struct Message {
    key: u64,
}

#[test]
fn test_drop_future_with_timers() {
    let mut sim = Simulation::new(42);

    let ctx1 = sim.create_context("first");

    sim.register_key_getter_for::<Message>(|msg| msg.key);

    sim.spawn(async move {
        {
            select! {
                _ = test_future(&ctx1, 1, 10.).fuse() => {
                    println!("received first")
                }
                _ = test_future(&ctx1, 2, 11.).fuse() => {
                    println!("received second")
                }
            };
        }
    });

    sim.step_until_no_events();

    assert_eq!(sim.time(), 10.);
}

#[test]
fn test_drop_completed_future_with_timers() {
    let mut sim = Simulation::new(42);

    let ctx1 = sim.create_context("first");
    let root = sim.create_context("root");
    let ctx1_id = ctx1.id();
    sim.register_key_getter_for::<Message>(|msg| msg.key);

    sim.spawn(async move {
        {
            select! {
                _ = test_future(&ctx1, 1, 10.).fuse() => {
                    println!("received first");
                }
                _ = test_future(&ctx1, 2, 11.).fuse() => {
                    println!("received second");
                }
            };
        }
    });

    root.emit(Message { key: 1 }, ctx1_id, 5.);

    sim.step_until_no_events();

    assert_eq!(sim.time(), 5.);
}

async fn test_future(ctx: &SimulationContext, key: EventKey, timeout: f64) -> AwaitResult<Message> {
    ctx.recv_event_by_key::<Message>(key).with_timeout(timeout).await
}
