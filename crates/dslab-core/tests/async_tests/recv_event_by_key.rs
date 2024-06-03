use std::rc::Rc;

use serde::Serialize;

use dslab_core::async_mode::EventKey;
use dslab_core::{cast, Event, Simulation, SimulationContext, StaticEventHandler};

#[derive(Clone, Serialize)]
struct TestEvent {
    key: u64,
}

fn get_event_key(e: &TestEvent) -> EventKey {
    e.key as EventKey
}

struct TestComponent {
    num_listeners: u32,
    iterations: u32,
    ctx: SimulationContext,
}

impl TestComponent {
    fn new(num_listeners: u32, iterations: u32, ctx: SimulationContext) -> Self {
        Self {
            num_listeners,
            iterations,
            ctx,
        }
    }

    fn start(self: Rc<Self>) {
        for i in 0..self.num_listeners {
            self.ctx.spawn(self.clone().listener(i as u64));
        }
        self.ctx.spawn(self.clone().sender());
    }

    async fn sender(self: Rc<Self>) {
        for _ in 0..self.iterations {
            for i in 0..self.num_listeners {
                self.ctx.emit_self_now(TestEvent { key: i as u64 });
            }
            self.ctx.sleep(10.).await;
        }
    }

    async fn listener(self: Rc<Self>, key: u64) {
        for i in 0..self.iterations {
            let event = self.ctx.recv_event_by_key_from_self::<TestEvent>(key).await;
            assert_eq!(event.src, self.ctx.id());
            assert_eq!(event.dst, self.ctx.id());
            assert_eq!(event.data.key, key);
            assert_eq!(event.time, (i * 10) as f64);
        }
        self.ctx.recv_event_by_key_from_self::<TestEvent>(key).await;
        panic!("This code must be unreachable");
    }
}

impl StaticEventHandler for TestComponent {
    fn on(self: Rc<Self>, event: Event) {
        cast!(match event.data {
            TestEvent { .. } => {
                panic!("Standard event handling must be unreachable");
            }
        })
    }
}

#[test]
fn test_recv_event_by_key() {
    let mut sim = Simulation::new(123);

    sim.register_key_getter_for::<TestEvent>(get_event_key);

    let comp_ctx = sim.create_context("comp");
    let comp = Rc::new(TestComponent::new(100, 100, comp_ctx));
    sim.add_static_handler("comp", comp.clone());

    comp.start();
    sim.step_until_no_events();
}
