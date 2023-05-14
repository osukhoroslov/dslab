use std::{cell::RefCell, rc::Rc};

use futures::{stream::FuturesUnordered, StreamExt};
use serde::Serialize;

use crate::{async_core::shared_state::AwaitResult, cast, EventHandler, Id, Simulation, SimulationContext};

#[test]
fn test_wait_for() {
    let mut sim = Simulation::new(42);
    let ctx = sim.create_context("dummy_worker");

    sim.spawn(async move {
        let time_wait_step = 5.;
        let concurrent_wait_items = 10;

        let start_time = ctx.time();
        assert!(start_time == 0.);

        ctx.async_wait_for(time_wait_step).await;

        assert!(ctx.time() == time_wait_step);

        let mut futures = FuturesUnordered::new();
        for i in 0..=concurrent_wait_items {
            futures.push(ctx.async_wait_for(i as f64 * time_wait_step));
        }

        let mut expected_next_time = time_wait_step;

        while let Some(_) = futures.next().await {
            assert!(ctx.time() == expected_next_time);
            expected_next_time += time_wait_step;
        }

        assert!(ctx.time() == ((concurrent_wait_items + 1) as f64 * time_wait_step));
    });

    sim.step_until_no_events();
}

struct SimpleListener {
    ctx: SimulationContext,
    system_id: Id,
    next_expected_message_id: u32,
    expect_timeout: bool,
    continue_listening: bool,
}

#[derive(Clone, Serialize)]
struct Message {
    id: u32,
}

#[derive(Clone, Serialize)]
struct Start {
    timeout: f64,
}

impl SimpleListener {
    fn on_start(&self, timeout: f64) {
        self.ctx.spawn(self.listen_with_timeout(timeout));
    }

    async fn listen_with_timeout(&self, timeout: f64) {
        while self.continue_listening {
            match self.ctx.async_wait_for_event::<Message>(self.system_id, timeout).await {
                AwaitResult::Ok((event, data)) => {
                    assert!(event.src == self.system_id);
                    assert!(!self.expect_timeout);
                    assert!(data.id == self.next_expected_message_id);
                }
                AwaitResult::Timeout(event) => {
                    assert!(event.src == self.system_id);
                    assert!(self.expect_timeout);
                }
            }
        }
    }
}

impl EventHandler for SimpleListener {
    fn on(&mut self, event: crate::Event) {
        cast!(match event.data {
            Start { timeout } => {
                self.on_start(timeout);
            }
            Message { id } => {
                assert!(id == self.next_expected_message_id);
            }
        })
    }
}

#[test]
fn test_async_wait_for_event() {
    let mut sim = Simulation::new(42);
    let message_timeout = 5.;

    let listener_context = sim.create_context("listener");
    let listener_id = listener_context.id();
    let system_context = sim.create_context("system");

    let listener = Rc::new(RefCell::new(SimpleListener {
        ctx: listener_context,
        system_id: system_context.id(),
        next_expected_message_id: 0,
        expect_timeout: false,
        continue_listening: true,
    }));

    sim.add_handler("listener", listener.clone());

    system_context.emit_now(
        Start {
            timeout: message_timeout,
        },
        listener_id,
    );

    listener.borrow_mut().expect_timeout = true;

    sim.step_until_time(100.);

    assert!(system_context.time() >= 100.);

    sim.spawn(async {
        let start_test_time = 202.;
        system_context
            .async_wait_for(start_test_time - system_context.time())
            .await;

        assert!(system_context.time() == start_test_time);

        let mut next_message_id = 1;

        listener.borrow_mut().expect_timeout = false;

        for _i in 0..=5 {
            next_message_id += 1;

            listener.borrow_mut().next_expected_message_id = next_message_id;

            system_context.emit_now(Message { id: next_message_id }, listener_id);

            system_context.async_wait_for(message_timeout - 1.).await;
        }

        listener.borrow_mut().expect_timeout = true;
        listener.borrow_mut().continue_listening = false;

        system_context.async_wait_for(100.).await;
        listener.borrow_mut().expect_timeout = false;

        for _i in 0..=5 {
            next_message_id += 1;

            listener.borrow_mut().next_expected_message_id = next_message_id;

            system_context.emit_now(Message { id: next_message_id }, listener_id);

            system_context.async_wait_for(message_timeout + 1.).await;
        }
    });

    sim.step_until_time(500.);
}
