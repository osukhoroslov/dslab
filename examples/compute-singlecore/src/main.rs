use env_logger::Builder;
use std::cell::RefCell;
use std::io::Write;
use std::rc::Rc;

use serde::Serialize;
use sugars::{rc, refcell};

use dslab_compute::singlecore::*;
use dslab_core::component::Id;
use dslab_core::context::SimulationContext;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;
use dslab_core::simulation::Simulation;
use dslab_core::{cast, log_error, log_info};

#[derive(Serialize)]
pub struct Start {}

pub struct Task {
    id: Id,
    compute: Rc<RefCell<Compute>>,
    flops: u64,
    memory: u64,
    ctx: SimulationContext,
}

impl Task {
    pub fn new(compute: Rc<RefCell<Compute>>, flops: u64, memory: u64, ctx: SimulationContext) -> Self {
        Self {
            id: ctx.id(),
            compute,
            flops,
            memory,
            ctx,
        }
    }
}

impl EventHandler for Task {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            Start {} => {
                log_info!(self.ctx, "received Start from {}", self.ctx.lookup_name(event.src));
                self.compute.borrow_mut().run(self.flops, self.memory, self.id);
            }
            CompStarted { id } => {
                log_info!(
                    self.ctx,
                    "received CompStarted from {} for {:?}",
                    self.ctx.lookup_name(event.src),
                    id
                );
            }
            CompFinished { id } => {
                log_info!(
                    self.ctx,
                    "received CompFinished from {} for {:?}",
                    self.ctx.lookup_name(event.src),
                    id
                );
            }
            CompFailed { id, reason } => {
                log_error!(
                    self.ctx,
                    "received CompFailed from {} for {:?}, because of {:?}",
                    self.ctx.lookup_name(event.src),
                    id,
                    reason
                );
            }
        })
    }
}

fn main() {
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    let mut sim = Simulation::new(123);

    let compute = rc!(refcell!(Compute::new(10, 1024, sim.create_context("compute"))));
    sim.add_handler("compute", compute.clone());

    let task1 = Task::new(compute.clone(), 100, 512, sim.create_context("task1"));
    let task1_id = sim.add_handler("task1", rc!(refcell!(task1)));
    let task2 = Task::new(compute, 200, 512, sim.create_context("task2"));
    let task2_id = sim.add_handler("task2", rc!(refcell!(task2)));

    let mut ctx = sim.create_context("root");
    ctx.emit(Start {}, task1_id, 0.);
    ctx.emit(Start {}, task2_id, 5.);

    sim.step_until_no_events();
}
