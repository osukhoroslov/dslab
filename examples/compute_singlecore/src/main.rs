use std::cell::RefCell;
use std::rc::Rc;

use serde::Serialize;
use sugars::{rc, refcell};

use compute::singlecore::*;
use core::cast;
use core::context::SimulationContext;
use core::event::Event;
use core::handler::EventHandler;
use core::simulation::Simulation;

#[derive(Serialize)]
pub struct Start {}

pub struct Task {
    id: String,
    compute: Rc<RefCell<Compute>>,
    flops: u64,
    memory: u64,
    ctx: SimulationContext,
}

impl Task {
    pub fn new(compute: Rc<RefCell<Compute>>, flops: u64, memory: u64, ctx: SimulationContext) -> Self {
        Self {
            id: ctx.id().to_string(),
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
                println!("{} [{}] received Start from {}", self.ctx.time(), self.id, event.src);
                self.compute.borrow_mut().run(self.flops, self.memory, &self.id);
            }
            CompStarted { id } => {
                println!(
                    "{} [{}] received CompStarted from {} for {:?}",
                    self.ctx.time(),
                    self.id,
                    event.src,
                    id
                );
            }
            CompFinished { id } => {
                println!(
                    "{} [{}] received CompFinished from {} for {:?}",
                    self.ctx.time(),
                    self.id,
                    event.src,
                    id
                );
            }
            CompFailed { id, reason } => {
                println!(
                    "{} [{}] received CompFailed from {} for {:?}, because of {:?}",
                    self.ctx.time(),
                    self.id,
                    event.src,
                    id,
                    reason
                );
            }
        })
    }
}

fn main() {
    let mut sim = Simulation::new(123);

    let compute = rc!(refcell!(Compute::new(10, 1024, sim.create_context("compute"))));
    sim.add_handler("compute", compute.clone());

    let task1 = Task::new(compute.clone(), 100, 512, sim.create_context("task1"));
    sim.add_handler("task1", rc!(refcell!(task1)));
    let task2 = Task::new(compute.clone(), 200, 512, sim.create_context("task2"));
    sim.add_handler("task2", rc!(refcell!(task2)));

    let mut ctx = sim.create_context("root");
    ctx.emit(Start {}, "task1", 0.);
    ctx.emit(Start {}, "task2", 5.);

    sim.step_until_no_events();
}
