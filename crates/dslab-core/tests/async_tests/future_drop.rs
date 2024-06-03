use std::cell::RefCell;
use std::rc::Rc;

use serde::Serialize;

use dslab_core::async_mode::EventKey;
use dslab_core::{cast, EventCancellationPolicy, Simulation, SimulationContext, StaticEventHandler};

#[derive(Clone, Serialize)]
struct TestEventWithRc {
    key: EventKey,
    #[serde(skip)]
    rc: Rc<RefCell<u32>>,
}

struct TestComponent {
    async_task_count: u32,
    on_handler_messages: RefCell<u32>,
    ctx: SimulationContext,
}

impl TestComponent {
    fn new(async_task_count: u32, ctx: SimulationContext) -> Self {
        Self {
            async_task_count,
            on_handler_messages: RefCell::new(0),
            ctx,
        }
    }

    fn start_waiting_for_events(self: Rc<Self>) {
        for i in 0..self.async_task_count {
            self.ctx.spawn(self.clone().wait_for_event_with_key(i as EventKey));
        }
    }

    fn start_waiting_for_timeouts(self: Rc<Self>, timeout: f64, rc: Rc<RefCell<u32>>) {
        for _ in 0..self.async_task_count {
            self.ctx.spawn(self.clone().wait_for_timeout(timeout, rc.clone()));
        }
    }

    async fn wait_for_event_with_key(self: Rc<Self>, key: EventKey) {
        let e = self.ctx.recv_event_by_key::<TestEventWithRc>(key).await;
        *e.data.rc.borrow_mut() += 1;
        self.ctx.sleep(10.).await;

        // Check that all async activities have received the copy of rc.
        assert_eq!(Rc::strong_count(&e.data.rc), 1 + self.async_task_count as usize);

        // Must never complete because we remove the event handler.
        let _ = self.ctx.recv_event_by_key::<TestEventWithRc>(key).await;
        panic!("This code must be unreachable");
    }

    async fn wait_for_timeout(self: Rc<Self>, timeout: f64, rc: Rc<RefCell<u32>>) {
        self.ctx.sleep(timeout).await;
        *rc.borrow_mut() += 1;

        // Check that all async activities have received the copy of rc.
        assert_eq!(Rc::strong_count(&rc), 1 + self.async_task_count as usize);

        // Must never complete because we remove the event handler.
        self.ctx.sleep(1000. * timeout).await;
        panic!("This code must be unreachable");
    }
}

impl StaticEventHandler for TestComponent {
    fn on(self: Rc<Self>, event: dslab_core::Event) {
        cast!(match event.data {
            TestEventWithRc { .. } => {
                *self.on_handler_messages.borrow_mut() += 1;
            }
        })
    }
}

#[test]
fn test_event_futures_drop_on_remove_handler() {
    let async_task_count = 10;

    let rc = Rc::new(RefCell::new(0));
    assert_eq!(Rc::strong_count(&rc), 1);

    let mut sim = Simulation::new(123);
    sim.register_key_getter_for::<TestEventWithRc>(|m| m.key);

    let comp_ctx = sim.create_context("comp");
    let comp = Rc::new(TestComponent::new(async_task_count, comp_ctx));
    let comp_id = sim.add_static_handler("comp", comp.clone());
    comp.clone().start_waiting_for_events();

    let root_ctx = sim.create_context("root");
    for i in 0..async_task_count {
        root_ctx.emit(
            TestEventWithRc {
                key: i as EventKey,
                rc: rc.clone(),
            },
            comp_id,
            10.,
        );
    }

    sim.step_until_no_events();

    assert_eq!(sim.time(), 20.);
    assert_eq!(sim.event_count(), async_task_count as u64);
    assert_eq!(Rc::strong_count(&rc), 1 + async_task_count as usize);
    assert_eq!(*rc.borrow(), async_task_count);

    sim.remove_handler("comp", EventCancellationPolicy::None);

    sim.step_until_no_events();

    assert_eq!(sim.time(), 20.);
    assert_eq!(sim.event_count(), async_task_count as u64);
    // Check that all async activities are dropped.
    assert_eq!(Rc::strong_count(&rc), 1);

    sim.add_static_handler("comp", comp.clone());

    assert_eq!(Rc::strong_count(&rc), 1);
    assert_eq!(*rc.borrow(), async_task_count);

    for i in 0..async_task_count {
        root_ctx.emit(
            TestEventWithRc {
                key: i as EventKey,
                rc: rc.clone(),
            },
            comp_id,
            5.,
        );
    }

    sim.step_until_no_events();

    assert_eq!(sim.time(), 25.);
    // Check that all new events are delivered via EventHandler.
    assert_eq!(*comp.on_handler_messages.borrow(), async_task_count);
}

#[test]
fn test_timer_futures_drop_on_remove_handler() {
    let async_task_count = 10;
    let timeout = 10.;

    let rc = Rc::new(RefCell::new(0));
    assert_eq!(Rc::strong_count(&rc), 1);

    let mut sim = Simulation::new(123);

    let comp_ctx = sim.create_context("comp");
    let comp = Rc::new(TestComponent::new(async_task_count, comp_ctx));
    sim.add_static_handler("comp", comp.clone());
    comp.clone().start_waiting_for_timeouts(timeout, rc.clone());

    sim.step_until_time(30.);

    assert_eq!(sim.time(), 30.);
    assert_eq!(Rc::strong_count(&rc), 1 + async_task_count as usize);
    assert_eq!(*rc.borrow(), async_task_count);

    sim.remove_handler("comp", EventCancellationPolicy::None);

    assert_eq!(Rc::strong_count(&rc), 1);

    sim.add_static_handler("comp", comp.clone());

    assert_eq!(Rc::strong_count(&rc), 1);
    assert_eq!(*rc.borrow(), async_task_count);

    sim.step_until_no_events();

    assert_eq!(sim.time(), 30.);
}
