use std::cell::RefCell;
use std::rc::Rc;

use serde::Serialize;

use dslab_core::async_mode::AwaitResult;
use dslab_core::{cast, Event, EventHandler, Id, Simulation, SimulationContext};

#[derive(Clone, Serialize)]
struct TestEvent {
    value: u32,
}

struct Listener {
    event_src: Id,
    timeout: f64,
    continue_listening: bool,
    expect_timeout: bool,
    next_expected_value: u32,
    ctx: SimulationContext,
}

impl Listener {
    fn new(event_src: Id, timeout: f64, ctx: SimulationContext) -> Self {
        Self {
            event_src,
            timeout,
            continue_listening: true,
            expect_timeout: false,
            next_expected_value: 0,
            ctx,
        }
    }

    fn start(&self) {
        self.ctx.spawn(self.listen_with_timeout());
    }

    async fn listen_with_timeout(&self) {
        while self.continue_listening {
            match self
                .ctx
                .recv_event_from::<TestEvent>(self.event_src)
                .with_timeout(self.timeout)
                .await
            {
                AwaitResult::Ok(event) => {
                    assert!(!self.expect_timeout);
                    assert_eq!(event.src, self.event_src);
                    assert_eq!(event.data.value, self.next_expected_value);
                }
                AwaitResult::Timeout {
                    src,
                    event_key,
                    timeout,
                } => {
                    assert!(self.expect_timeout);
                    assert_eq!(src, Some(self.event_src));
                    assert_eq!(event_key, None);
                    assert_eq!(timeout, self.timeout);
                }
            }
        }
    }
}

impl EventHandler for Listener {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            TestEvent { value } => {
                assert_eq!(value, self.next_expected_value);
            }
        })
    }
}

#[test]
fn test_recv_event() {
    let mut sim = Simulation::new(123);
    let timeout = 5.;

    let ctx = sim.create_context("main");

    let listener = Rc::new(RefCell::new(Listener::new(
        ctx.id(),
        timeout,
        sim.create_context("listener"),
    )));
    let listener_id = sim.add_handler("listener", listener.clone());
    listener.borrow().start();

    listener.borrow_mut().expect_timeout = true;
    sim.step_until_time(100.);
    assert_eq!(ctx.time(), 100.);

    sim.spawn(async move {
        let start_time = 202.;
        ctx.sleep(start_time - ctx.time()).await;
        assert_eq!(ctx.time(), start_time);

        listener.borrow_mut().expect_timeout = false;

        let mut next_value = 1;
        for _ in 0..=5 {
            next_value += 1;
            listener.borrow_mut().next_expected_value = next_value;
            ctx.emit_now(TestEvent { value: next_value }, listener_id);
            ctx.sleep(timeout - 1.).await;
        }

        listener.borrow_mut().expect_timeout = true;
        listener.borrow_mut().continue_listening = false;

        ctx.sleep(100.).await;

        listener.borrow_mut().expect_timeout = false;

        for _ in 0..=5 {
            next_value += 1;
            listener.borrow_mut().next_expected_value = next_value;
            ctx.emit_now(TestEvent { value: next_value }, listener_id);
            ctx.sleep(timeout + 1.).await;
        }
    });

    sim.step_until_time(500.);
}
