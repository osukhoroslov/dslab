use std::cell::RefCell;
use std::rc::Rc;

use serde::Serialize;

use dslab_core::{cast, Event, EventHandler, Id, Simulation, SimulationContext};

// Event types
#[derive(Clone, Serialize)]
struct Request {
    time: f64,
}

#[derive(Clone, Serialize)]
struct Response {
    req_time: f64,
}

// Component implementation
struct Process {
    net_delay: f64,
    ctx: SimulationContext,
}

impl Process {
    pub fn new(net_delay: f64, ctx: SimulationContext) -> Self {
        Self { net_delay, ctx }
    }

    fn send_request(&self, dst: Id) {
        self.ctx.emit(Request { time: self.ctx.time() }, dst, self.net_delay);
    }

    fn on_request(&self, src: Id, req_time: f64) {
        let proc_delay = self.ctx.gen_range(0.5..1.0);
        self.ctx.emit(Response { req_time }, src, proc_delay + self.net_delay);
    }

    fn on_response(&self, req_time: f64) {
        let response_time = self.ctx.time() - req_time;
        println!("Response time: {:.2}", response_time);
    }
}

impl EventHandler for Process {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            Request { time } => {
                self.on_request(event.src, time)
            }
            Response { req_time } => {
                self.on_response(req_time)
            }
        })
    }
}

pub fn run_callbacks_example() {
    // Create simulation with specified random seed
    let mut sim = Simulation::new(123);

    // Create and register components
    let proc1 = Process::new(0.1, sim.create_context("proc1"));
    let proc1_ref = Rc::new(RefCell::new(proc1));
    sim.add_handler("proc1", proc1_ref.clone());
    let proc2 = Process::new(0.1, sim.create_context("proc2"));
    let proc2_ref = Rc::new(RefCell::new(proc2));
    let proc2_id = sim.add_handler("proc2", proc2_ref);

    // Ask proc1 to send request to proc2
    proc1_ref.borrow().send_request(proc2_id);

    // Run simulation until there are no pending events
    sim.step_until_no_events();
    println!("Simulation time: {:.2}", sim.time());
}
