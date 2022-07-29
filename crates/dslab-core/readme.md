# DSLab Simulation Core

A compact library for discrete-event simulation. 

This library provides a generic discrete-event simulation engine. It can be used to implement arbitrary simulations consisting of user-defined _components_ producing and consuming user-defined _events_. It serves as a foundation for other parts of DSLab framework. Being generic and versatile, it can also be used outside DSLab and distributed systems domain.

The simulation is configured and managed via [`Simulation`], which includes methods for registering simulation components, stepping through the simulation, obtaining the current simulation time, etc. The library manages simulation state, which includes clock, event queue and random number generator. The latter is initialized with user-defined seed to ensure deterministic execution and reproduction of results. 

It is possible to use any user-defined Rust types as simulation components. The components access simulation state and produce events via [`SimulationContext`]. Each component typically uses a unique simulation context, which allows to differentiate events produced by different components. To be able to consume events, the component should implement the [`EventHandler`] trait, which is invoked to pass events to the component. Each simulation component is registered with unique name and identifier, which can be used for specifying the event source or destination, logging purposes, etc.

The simulation represents a sequence of events. Each event has a unique identifier, timestamp, source, destination and user-defined payload. The library supports using arbitrary serializable types as event payloads, the structure of payload is opaque to the library. The events are processed by retrieving the next event from the queue ordered by event timestamps, advancing the simulation clock to the event time and invoking the EventHandler implementation of component specified as the event destination. When processing the event, the component can create and emit new events with arbitrary future timestamps via its SimulationContext. The new events are placed in the event queue for further processing. It is also possible to cancel the previously emitted events before they are processed.

The library also provides convenient facilities for logging of events or arbitrary messages during the simulation with inclusion of component names, logging levels, etc.

## Example

```rust
use std::cell::RefCell;
use std::rc::Rc;
use serde::Serialize;
use dslab_core::{cast, Event, EventHandler, Id, Simulation, SimulationContext};

// Event types (should implement Serialize)
#[derive(Serialize)]
pub struct Ping {
    payload: f64,
}

#[derive(Serialize)]
pub struct Pong {
    payload: f64,
}

// Simulation component types (here we have a single one) 
pub struct Process {
    ctx: SimulationContext,
}

impl Process {
    pub fn new(ctx: SimulationContext) -> Self {
        Self { ctx }
    }

    fn send_ping(&mut self, dst: Id) {
        let payload = self.ctx.time() + 0.5;
        self.ctx.emit(Ping { payload }, dst, 0.5);
    }
}

impl EventHandler for Process {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            Ping { payload } => {
                assert_eq!(self.ctx.time(), payload);
                let payload = self.ctx.time() + 1.2;
                self.ctx.emit(Pong { payload }, event.src, 1.2);
            }
            Pong { payload } => {
                assert_eq!(self.ctx.time(), payload);
            }
        })
    }
}

// Simulation setup and execution
fn main() {
    let mut sim = Simulation::new(123);
    let pinger = Rc::new(RefCell::new(Process::new(sim.create_context("pinger"))));
    let _pinger_id = sim.add_handler("pinger", pinger.clone());
    let ponger = Rc::new(RefCell::new(Process::new(sim.create_context("ponger"))));
    let ponger_id = sim.add_handler("ponger", ponger.clone());
    pinger.borrow_mut().send_ping(ponger_id);
    sim.step_until_no_events();
    assert_eq!(sim.time(), 1.7)
}
```
