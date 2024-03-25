use std::{cell::RefCell, rc::Rc};

use serde::Serialize;

use dslab_core::{cast, Event, EventHandler, Id, Simulation, SimulationContext};

struct TestEventHandler {
    pub sim: Rc<RefCell<Simulation>>,
}

impl TestEventHandler {
    fn register_new_component(&mut self, component_name: String) -> SimulationContext {
        let context = self.sim.borrow().create_context(&component_name);
        let new_event_handler = Rc::new(RefCell::new(TestEventHandler { sim: self.sim.clone() }));
        self.sim
            .borrow()
            .add_handler(&component_name, new_event_handler.clone());
        context
    }
}

impl EventHandler for TestEventHandler {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            TestEventData {
                component_name,
                create_nested_components,
            } => {
                let context = self.register_new_component(component_name);
                if create_nested_components {
                    context.emit_self_now(TestEventData {
                        component_name: "component1".to_string(),
                        create_nested_components: false,
                    });
                }
            }
        })
    }
}

#[derive(Clone, Serialize)]
struct TestEventData {
    component_name: String,
    create_nested_components: bool,
}

fn run_test_simulation(create_nested_components: bool) {
    let sim = Rc::new(RefCell::new(Simulation::new(0)));

    let event_handler = Rc::new(RefCell::new(TestEventHandler { sim: sim.clone() }));

    let context = sim.borrow_mut().create_context("test_event_handler");
    sim.borrow_mut()
        .add_handler("test_event_handler", event_handler.clone());

    context.emit_self_now(TestEventData {
        component_name: "component1".to_string(),
        create_nested_components,
    });

    sim.borrow().step_until_no_events();
}

#[test]
fn test_creation_of_simulation_components_inside_event_handlers() {
    run_test_simulation(false);
}

#[test]
fn test_create_simulation_component_from_newly_created_one() {
    run_test_simulation(true);
}
