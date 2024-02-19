use std::{cell::RefCell, rc::Rc};

use serde::Serialize;

use dslab_core::{async_core::await_details::EventKey, cast, Event, EventHandler, Simulation, SimulationContext};

#[derive(Clone, Serialize)]
struct Message {
    key: u64,
}

fn get_message_key(msg: &Message) -> EventKey {
    msg.key as EventKey
}

#[derive(Clone, Serialize)]
struct Start {
    handlers: u32,
    iterations: u32,
}

struct SimpleExchanger {
    ctx: SimulationContext,
}

impl SimpleExchanger {
    fn on_start(&self, handlers: u32, iterations: u32) {
        self.ctx.spawn(self.spawner(handlers, iterations));
        for i in 0..handlers {
            self.ctx.spawn(self.listener(i as u64, iterations));
        }
    }

    async fn spawner(&self, handlers: u32, iterations: u32) {
        for _i in 0..iterations {
            for i in 0..handlers {
                self.ctx.emit_self_now(Message { key: i as u64 });
            }
            self.ctx.sleep(10.).await;
        }
    }

    async fn listener(&self, key: u64, iterations: u32) {
        for _i in 0..iterations {
            let (event, data) = self.ctx.recv_event_by_key_from_self::<Message>(key).await;
            assert!(event.src == self.ctx.id());
            assert!(event.dst == self.ctx.id());
            assert!(data.key == key);
        }

        self.ctx.recv_event_by_key_from_self::<Message>(key).await;

        panic!("unreachable handle");
    }
}

impl EventHandler for SimpleExchanger {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            Message { key: _ } => {
                panic!("standard event handling must be unreachable");
            }
            Start { handlers, iterations } => {
                self.on_start(handlers, iterations);
            }
        })
    }
}

#[test]
fn async_wait_for_details_test() {
    let mut sim = Simulation::new(42);

    sim.register_key_getter_for::<Message>(get_message_key);

    let exchanger_context = sim.create_context("exchanger");
    let exchanger_id = exchanger_context.id();

    let exchanger = Rc::new(RefCell::new(SimpleExchanger { ctx: exchanger_context }));

    sim.add_handler("exchanger", exchanger.clone());
    let root_context = sim.create_context("root");

    root_context.emit_now(
        Start {
            handlers: 100,
            iterations: 100,
        },
        exchanger_id,
    );

    sim.step_until_no_events();
}
