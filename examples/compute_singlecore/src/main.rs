use std::cell::RefCell;
use std::rc::Rc;

use compute::singlecore::*;
use core::actor::{Actor, ActorContext, ActorId, Event};
use core::cast;
use core::sim::Simulation;

#[derive(Debug, Clone)]
pub struct Start {}

pub struct TaskActor {
    flops: u64,
    memory: u64,
}

impl TaskActor {
    pub fn new(flops: u64, memory: u64) -> Self {
        Self { flops, memory }
    }
}

impl Actor for TaskActor {
    fn on(&mut self, event: Box<dyn Event>, from: ActorId, ctx: &mut ActorContext) {
        cast!(match event {
            Start {} => {
                println!("{} [{}] received Start from {}", ctx.time(), ctx.id, from);
                ctx.emit(
                    CompRequest {
                        flops: self.flops,
                        memory: self.memory,
                    },
                    ActorId::from("compute"),
                    0.,
                );
            }
            CompStarted { id } => {
                println!(
                    "{} [{}] received CompStarted from {} for {:?}",
                    ctx.time(),
                    ctx.id,
                    from,
                    id
                );
            }
            CompFinished { id } => {
                println!(
                    "{} [{}] received CompFinished from {} for {:?}",
                    ctx.time(),
                    ctx.id,
                    from,
                    id
                );
            }
            CompFailed { id, reason } => {
                println!(
                    "{} [{}] received CompFailed from {} for {:?}, because of {:?}",
                    ctx.time(),
                    ctx.id,
                    from,
                    id,
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
    sim.add_actor("task1", Rc::new(RefCell::new(TaskActor::new(100, 512))));
    sim.add_actor("task2", Rc::new(RefCell::new(TaskActor::new(200, 512))));
    sim.add_actor("compute", Rc::new(RefCell::new(Compute::new(10, 1024))));
    sim.add_event(Start {}, ActorId::from("0"), ActorId::from("task1"), 0.);
    sim.add_event(Start {}, ActorId::from("0"), ActorId::from("task2"), 5.);
    sim.step_until_no_events();
}
