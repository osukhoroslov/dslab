use std::{cell::RefCell, rc::Rc};

use dslab_core::{event::EventId, handler::EventCancellation, Event, EventHandler, Simulation, SimulationContext};
use serde::Serialize;

#[derive(Clone, Serialize)]
struct TestEvent {}

struct TestComponent {}

impl EventHandler for TestComponent {
    fn on(&mut self, _: Event) {}
}

fn prepare_test(first_component: &str, second_component: &str) -> Simulation {
    let mut sim = Simulation::new(42);

    for name in [first_component, second_component] {
        let cmp = Rc::new(RefCell::new(TestComponent {}));
        sim.add_handler(name, cmp);
    }

    let ctx = sim.create_context("master");
    let first_id = sim.lookup_id(first_component);
    let second_id = sim.lookup_id(second_component);

    ctx.emit_as(TestEvent {}, first_id, second_id, 0.);
    ctx.emit_as(TestEvent {}, second_id, first_id, 0.);
    ctx.emit_as(TestEvent {}, second_id, second_id, 0.);
    ctx.emit_as(TestEvent {}, first_id, first_id, 0.);

    sim
}

#[test]
fn cancel_nothing() {
    let mut sim = prepare_test("first", "second");

    let events = sim.dump_events();
    sim.remove_handler("first", EventCancellation::None);

    assert_eq!(sim.dump_events().len(), events.len());
}

#[test]
fn cancel_incoming() {
    let mut sim = prepare_test("first", "second");

    let first_id = sim.lookup_id("first");
    let mut event_ids_to_check = sim
        .dump_events()
        .iter()
        .filter(|e| e.dst != first_id)
        .map(|e| e.id)
        .collect::<Vec<EventId>>();

    event_ids_to_check.sort();

    sim.remove_handler("first", EventCancellation::Incoming);

    let mut events_after = sim.dump_events().iter().map(|e| e.id).collect::<Vec<EventId>>();
    events_after.sort();

    assert_eq!(event_ids_to_check, events_after);
}

#[test]
fn cancel_outgoing() {
    let mut sim = prepare_test("first", "second");

    let first_id = sim.lookup_id("first");
    let mut event_ids_to_check = sim
        .dump_events()
        .iter()
        .filter(|e| e.src != first_id)
        .map(|e| e.id)
        .collect::<Vec<EventId>>();

    event_ids_to_check.sort();

    sim.remove_handler("first", EventCancellation::Outgoing);

    let mut events_after = sim.dump_events().iter().map(|e| e.id).collect::<Vec<EventId>>();
    events_after.sort();

    assert_eq!(event_ids_to_check, events_after);
}

#[test]
fn cancel_both() {
    let mut sim = prepare_test("first", "second");

    let first_id = sim.lookup_id("first");
    let mut event_ids_to_check = sim
        .dump_events()
        .iter()
        .filter(|e| e.src != first_id && e.dst != first_id)
        .map(|e| e.id)
        .collect::<Vec<EventId>>();

    event_ids_to_check.sort();

    sim.remove_handler("first", EventCancellation::Both);

    let mut events_after = sim.dump_events().iter().map(|e| e.id).collect::<Vec<EventId>>();
    events_after.sort();

    assert_eq!(event_ids_to_check, events_after);
}
