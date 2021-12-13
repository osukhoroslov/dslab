use std::cell::RefCell;
use std::rc::Rc;

use compute::computation::Computation;
use compute::singlecore::*;
use core::actor::{Actor, ActorContext, ActorId, Event};
use core::match_event;
use core::sim::Simulation;

#[derive(Debug, Clone)]
pub struct Start {}

pub struct TaskActor {
    task: Computation,
}

impl TaskActor {
    pub fn new(computation: Computation) -> Self {
        Self { task: computation }
    }
}

impl Actor for TaskActor {
    fn on(&mut self, event: Box<dyn Event>, from: ActorId, ctx: &mut ActorContext) {
        match_event!( event {
            Start {} => {
                println!("{} [{}] received Start from {}", ctx.time(), ctx.id, from);
                ctx.emit(
                    CompRequest {
                        computation: self.task.clone(),
                    },
                    ActorId::from("compute"),
                    0.,
                );
            },
            CompStarted { computation } => {
                println!(
                    "{} [{}] received CompStarted from {} for {:?}",
                    ctx.time(),
                    ctx.id,
                    from,
                    computation
                );
            },
            CompFinished { computation } => {
                println!(
                    "{} [{}] received CompFinished from {} for {:?}",
                    ctx.time(),
                    ctx.id,
                    from,
                    computation
                );
            },
            CompFailed { computation, reason } => {
                println!(
                    "{} [{}] received CompFailed from {} for {:?}, because of {:?}",
                    ctx.time(),
                    ctx.id,
                    from,
                    computation,
                    reason
                );
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}

fn main() {
    let mut sim = Simulation::new(123);
    sim.add_actor(
        "task1",
        Rc::new(RefCell::new(TaskActor::new(Computation::new(100, 512, 1)))),
    );
    sim.add_actor(
        "task2",
        Rc::new(RefCell::new(TaskActor::new(Computation::new(200, 512, 2)))),
    );
    sim.add_actor("compute", Rc::new(RefCell::new(ComputeActor::new(10, 1024))));
    sim.add_event(Start {}, ActorId::from("0"), ActorId::from("task1"), 0.);
    sim.add_event(Start {}, ActorId::from("0"), ActorId::from("task2"), 5.);
    sim.step_until_no_events();
}
