//! Tests of event cancellation policies on event handler removal.

use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;

use serde::Serialize;

use dslab_core::{Event, EventCancellationPolicy, EventHandler, EventId, Simulation};

#[derive(Clone, Serialize)]
struct TestEvent {}

struct TestComponent {}

impl EventHandler for TestComponent {
    fn on(&mut self, _: Event) {}
}

fn prepare_test(comp1_name: &str, comp2_name: &str) -> Simulation {
    let mut sim = Simulation::new(123);

    for name in [comp1_name, comp2_name] {
        let comp = Rc::new(RefCell::new(TestComponent {}));
        sim.add_handler(name, comp);
    }
    let comp1_id = sim.lookup_id(comp1_name);
    let comp2_id = sim.lookup_id(comp2_name);

    let ctx = sim.create_context("main");
    ctx.emit_as(TestEvent {}, comp1_id, comp2_id, 0.);
    ctx.emit_as(TestEvent {}, comp2_id, comp1_id, 0.);
    ctx.emit_as(TestEvent {}, comp2_id, comp2_id, 0.);
    ctx.emit_as(TestEvent {}, comp1_id, comp1_id, 0.);

    sim
}

#[test]
fn test_none_policy() {
    let mut sim = prepare_test("comp1", "comp2");

    let events = sim.dump_events();
    sim.remove_handler("comp1", EventCancellationPolicy::None);

    assert_eq!(sim.dump_events().len(), events.len());
}

#[test]
fn test_incoming_policy() {
    let mut sim = prepare_test("comp1", "comp2");

    let comp1_id = sim.lookup_id("comp1");
    let expected_event_ids = sim
        .dump_events()
        .iter()
        .filter(|e| e.dst != comp1_id)
        .map(|e| e.id)
        .collect::<HashSet<EventId>>();

    sim.remove_handler("comp1", EventCancellationPolicy::Incoming);

    let left_event_ids = sim.dump_events().iter().map(|e| e.id).collect::<HashSet<EventId>>();
    assert_eq!(left_event_ids, expected_event_ids);
}

#[test]
fn test_outgoing_policy() {
    let mut sim = prepare_test("comp1", "comp2");

    let comp1_id = sim.lookup_id("comp1");
    let expected_event_ids = sim
        .dump_events()
        .iter()
        .filter(|e| e.src != comp1_id)
        .map(|e| e.id)
        .collect::<HashSet<EventId>>();

    sim.remove_handler("comp1", EventCancellationPolicy::Outgoing);

    let left_event_ids = sim.dump_events().iter().map(|e| e.id).collect::<HashSet<EventId>>();
    assert_eq!(left_event_ids, expected_event_ids);
}

#[test]
fn test_all_policy() {
    let mut sim = prepare_test("comp1", "comp2");

    let comp1_id = sim.lookup_id("comp1");
    let expected_event_ids = sim
        .dump_events()
        .iter()
        .filter(|e| e.src != comp1_id && e.dst != comp1_id)
        .map(|e| e.id)
        .collect::<HashSet<EventId>>();

    sim.remove_handler("comp1", EventCancellationPolicy::All);

    let left_event_ids = sim.dump_events().iter().map(|e| e.id).collect::<HashSet<EventId>>();
    assert_eq!(expected_event_ids, left_event_ids);
}
