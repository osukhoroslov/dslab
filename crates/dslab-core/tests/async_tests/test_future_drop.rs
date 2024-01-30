use std::cell::RefCell;
use std::rc::Rc;

use futures::select;
use futures::FutureExt;
use serde::Serialize;

use dslab_core::async_core::{AwaitResult, EventKey};
use dslab_core::{cast, EventCancellationPolicy, EventHandler, Simulation, SimulationContext};

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

#[derive(Clone, Serialize)]
struct MessageWithRc {
    key: EventKey,
    #[serde(skip)]
    rc: Rc<RefCell<u32>>,
}

#[derive(Clone, Serialize)]
struct Start {
    async_count: u32,
}

struct SimpleComponent {
    ctx: SimulationContext,
    on_handler_messages: u32,
}

impl SimpleComponent {
    async fn wait_on_key(&self, key: EventKey, async_count: u32) {
        let (_, data) = self.ctx.recv_event_by_key::<MessageWithRc>(key).await;
        *data.rc.borrow_mut() += 1;
        self.ctx.sleep(10.).await;

        // Check that all async activities have received the copy of rc.
        assert_eq!(Rc::strong_count(&data.rc), 1 + async_count as usize);

        let (_, data) = self.ctx.recv_event_by_key::<MessageWithRc>(key).await;
        *data.rc.borrow_mut() -= 1;
    }
}

impl EventHandler for SimpleComponent {
    fn on(&mut self, event: dslab_core::Event) {
        cast!(match event.data {
            Start { async_count } => {
                for i in 0..async_count {
                    self.ctx.spawn(self.wait_on_key(i as EventKey, async_count));
                }
            }
            MessageWithRc { .. } => {
                self.on_handler_messages += 1;
            }
        })
    }
}

#[test]
fn test_futures_drop_on_remove_handler() {
    let mut sim = Simulation::new(42);

    sim.register_key_getter_for::<MessageWithRc>(|m| m.key);

    let component_ctx = sim.create_context("simple_component");
    let root_ctx = sim.create_context("root");

    let component_id = component_ctx.id();

    let component = Rc::new(RefCell::new(SimpleComponent {
        ctx: component_ctx,
        on_handler_messages: 0,
    }));

    sim.add_handler("simple_component", component.clone());

    let async_count = 10;
    root_ctx.emit(Start { async_count }, component_id, 5.);

    let rc = Rc::new(RefCell::new(0));

    for i in 0..async_count {
        root_ctx.emit(
            MessageWithRc {
                key: i as EventKey,
                rc: rc.clone(),
            },
            component_id,
            10.,
        );
    }

    sim.step_until_no_events();

    assert_eq!(sim.time(), 20.);
    assert_eq!(Rc::strong_count(&rc), 1 + async_count as usize);
    assert_eq!(*rc.borrow(), async_count);
    assert_eq!(sim.event_count(), 1 + async_count as u64);

    sim.remove_handler("simple_component", EventCancellationPolicy::None);

    sim.step_until_no_events();

    assert_eq!(sim.time(), 20.);
    assert_eq!(sim.event_count(), 1 + async_count as u64);

    sim.add_handler("simple_component", component.clone());

    // Check that all async activities are dropped.
    assert_eq!(Rc::strong_count(&rc), 1);
    assert_eq!(*rc.borrow(), async_count);

    for i in 0..async_count {
        root_ctx.emit(
            MessageWithRc {
                key: i as EventKey,
                rc: rc.clone(),
            },
            component_id,
            5.,
        );
    }

    sim.step_until_no_events();

    assert_eq!(sim.time(), 25.);
    // Check that all new events are delivered via EventHandler.
    assert_eq!(component.borrow().on_handler_messages, async_count);
}
