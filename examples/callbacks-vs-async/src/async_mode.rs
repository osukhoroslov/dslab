use std::rc::Rc;

use serde::Serialize;

use dslab_core::{cast, Event, Id, Simulation, SimulationContext, StaticEventHandler};

// Event types
#[derive(Clone, Serialize)]
struct Request {}

#[derive(Clone, Serialize)]
struct Response {}

// Component implementation
struct Process {
    net_delay: f64,
    ctx: SimulationContext,
}

impl Process {
    pub fn new(net_delay: f64, ctx: SimulationContext) -> Self {
        Self { net_delay, ctx }
    }

    fn send_request(self: Rc<Self>, dst: Id) {
        self.ctx.spawn(self.clone().send_request_and_get_response(dst))
    }

    async fn send_request_and_get_response(self: Rc<Self>, dst: Id) {
        let send_time = self.ctx.time();
        self.ctx.emit(Request {}, dst, self.net_delay);
        self.ctx.recv_event::<Response>().await;
        let response_time = self.ctx.time() - send_time;
        println!("Response time: {:.2}", response_time);
    }

    async fn process_request(self: Rc<Self>, src: Id) {
        self.ctx.sleep(self.ctx.gen_range(0.5..1.0)).await;
        self.ctx.emit(Response {}, src, self.net_delay);
    }
}

impl StaticEventHandler for Process {
    fn on(self: Rc<Self>, event: Event) {
        cast!(match event.data {
            Request {} => {
                self.ctx.spawn(self.clone().process_request(event.src))
            }
        })
    }
}

pub fn run_async_example() {
    // Create simulation with specified random seed
    let mut sim = Simulation::new(123);

    // Create and register components
    let proc1 = Process::new(0.1, sim.create_context("proc1"));
    let proc1_ref = Rc::new(proc1);
    sim.add_static_handler("proc1", proc1_ref.clone());
    let proc2 = Process::new(0.1, sim.create_context("proc2"));
    let proc2_ref = Rc::new(proc2);
    let proc2_id = sim.add_static_handler("proc2", proc2_ref);

    // Ask proc1 to emit a Request event to proc2
    proc1_ref.send_request(proc2_id);

    // Run simulation until there are no pending events
    sim.step_until_no_events();
    println!("Simulation time: {:.2}", sim.time());
}
