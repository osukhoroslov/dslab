use std::cell::RefCell;
use std::rc::Rc;

use serde::Serialize;

use dslab_core::async_mode::AwaitResult;
use dslab_core::{cast, Event, Id, Simulation, SimulationContext, StaticEventHandler};

#[derive(Clone, Serialize)]
struct TestEvent {
    value: u32,
}

struct Listener {
    event_src: Id,
    timeout: f64,
    continue_listening: RefCell<bool>,
    expect_timeout: RefCell<bool>,
    next_expected_value: RefCell<u32>,
    ctx: SimulationContext,
}

impl Listener {
    fn new(event_src: Id, timeout: f64, ctx: SimulationContext) -> Self {
        Self {
            event_src,
            timeout,
            continue_listening: RefCell::new(true),
            expect_timeout: RefCell::new(false),
            next_expected_value: RefCell::new(0),
            ctx,
        }
    }

    fn start(self: Rc<Self>) {
        self.ctx.spawn(self.clone().listen_with_timeout());
    }

    async fn listen_with_timeout(self: Rc<Self>) {
        while *self.continue_listening.borrow() {
            match self
                .ctx
                .recv_event_from::<TestEvent>(self.event_src)
                .with_timeout(self.timeout)
                .await
            {
                AwaitResult::Ok(event) => {
                    assert!(!*self.expect_timeout.borrow());
                    assert_eq!(event.src, self.event_src);
                    assert_eq!(event.data.value, *self.next_expected_value.borrow());
                }
                AwaitResult::Timeout {
                    src,
                    event_key,
                    timeout,
                } => {
                    assert!(*self.expect_timeout.borrow());
                    assert_eq!(src, Some(self.event_src));
                    assert_eq!(event_key, None);
                    assert_eq!(timeout, self.timeout);
                }
            }
        }
    }
}

impl StaticEventHandler for Listener {
    fn on(self: Rc<Self>, event: Event) {
        cast!(match event.data {
            TestEvent { value } => {
                assert_eq!(value, *self.next_expected_value.borrow());
            }
        })
    }
}

#[test]
fn test_recv_event() {
    let mut sim = Simulation::new(123);
    let timeout = 5.;

    let ctx = sim.create_context("main");

    let listener = Rc::new(Listener::new(ctx.id(), timeout, sim.create_context("listener")));
    let listener_id = sim.add_static_handler("listener", listener.clone());
    listener.clone().start();

    *listener.expect_timeout.borrow_mut() = true;
    sim.step_until_time(100.);
    assert_eq!(ctx.time(), 100.);

    sim.spawn(async move {
        let start_time = 202.;
        ctx.sleep(start_time - ctx.time()).await;
        assert_eq!(ctx.time(), start_time);

        *listener.expect_timeout.borrow_mut() = false;

        let mut next_value = 1;
        for _ in 0..=5 {
            next_value += 1;
            *listener.next_expected_value.borrow_mut() = next_value;
            ctx.emit_now(TestEvent { value: next_value }, listener_id);
            ctx.sleep(timeout - 1.).await;
        }

        *listener.expect_timeout.borrow_mut() = true;
        *listener.continue_listening.borrow_mut() = false;

        ctx.sleep(100.).await;

        *listener.expect_timeout.borrow_mut() = false;

        for _ in 0..=5 {
            next_value += 1;
            *listener.next_expected_value.borrow_mut() = next_value;
            ctx.emit_now(TestEvent { value: next_value }, listener_id);
            ctx.sleep(timeout + 1.).await;
        }
    });

    sim.step_until_time(500.);
}
