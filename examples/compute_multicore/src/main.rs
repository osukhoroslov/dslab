use std::cell::RefCell;
use std::rc::Rc;

use compute::multicore::*;
use core::actor::{Actor, ActorContext, ActorId, Event};
use core::cast;
use core::sim::Simulation;

#[derive(Debug, Clone)]
pub struct Start {}

pub struct TaskActor {
    flops: u64,
    memory: u64,
    min_cores: u64,
    max_cores: u64,
    cores_dependency: CoresDependency,
}

impl TaskActor {
    pub fn new(flops: u64, memory: u64, min_cores: u64, max_cores: u64, cores_dependency: CoresDependency) -> Self {
        Self {
            flops,
            memory,
            min_cores,
            max_cores,
            cores_dependency,
        }
    }
}

impl Actor for TaskActor {
    fn on(&mut self, event: Box<dyn Event>, from: ActorId, ctx: &mut ActorContext) {
        cast!(match event {
            Start {} => {
                println!("{} [{}] received Start from {}", ctx.time(), ctx.id, from);
                let compute_actor = ActorId::from("compute");
                ctx.emit(
                    CompRequest {
                        flops: self.flops,
                        memory: self.memory,
                        min_cores: self.min_cores,
                        max_cores: self.max_cores,
                        cores_dependency: self.cores_dependency.clone(),
                    },
                    compute_actor,
                    0.,
                );
            }
            CompStarted { id, cores } => {
                println!(
                    "{} [{}] received CompStarted from {} for {:?} on {} cores",
                    ctx.time(),
                    ctx.id,
                    from,
                    id,
                    cores
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
                    "{} [{}] received CompFailed from {} for {:?} with reason {:?}",
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

pub struct AllocationActor {
    allocation: Allocation,
    time: f64,
}

impl AllocationActor {
    pub fn new(allocation: Allocation, time: f64) -> Self {
        Self {
            allocation: allocation,
            time: time,
        }
    }
}

impl Actor for AllocationActor {
    fn on(&mut self, event: Box<dyn Event>, from: ActorId, ctx: &mut ActorContext) {
        cast!(match event {
            Start {} => {
                println!("{} [{}] received Start from {}", ctx.time(), ctx.id, from);
                let compute_actor = ActorId::from("compute");
                ctx.emit(
                    AllocationRequest {
                        allocation: self.allocation.clone(),
                    },
                    compute_actor.clone(),
                    0.,
                );
                ctx.emit(
                    DeallocationRequest {
                        allocation: self.allocation.clone(),
                    },
                    compute_actor,
                    self.time,
                );
            }
            AllocationFailed { id, reason } => {
                println!(
                    "{} [{}] received AllocationFailed from {} for {:?} with reason {:?}",
                    ctx.time(),
                    ctx.id,
                    from,
                    id,
                    reason
                );
            }
            DeallocationFailed { id, reason } => {
                println!(
                    "{} [{}] received DeallocationFailed from {} for {:?} with reason {:?}",
                    ctx.time(),
                    ctx.id,
                    from,
                    id,
                    reason
                );
            }
            AllocationSuccess { id } => {
                println!(
                    "{} [{}] received AllocationSuccess from {} for {:?}",
                    ctx.time(),
                    ctx.id,
                    from,
                    id
                );
            }
            DeallocationSuccess { id } => {
                println!(
                    "{} [{}] received DeallocationSuccess from {} for {:?}",
                    ctx.time(),
                    ctx.id,
                    from,
                    id
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
        Rc::new(RefCell::new(TaskActor::new(100, 512, 2, 6, CoresDependency::Linear))),
    );
    sim.add_actor(
        "task2",
        Rc::new(RefCell::new(TaskActor::new(
            100,
            512,
            4,
            10,
            CoresDependency::LinearWithFixed { fixed_part: 0.4 },
        ))),
    );
    sim.add_actor(
        "task3",
        Rc::new(RefCell::new(TaskActor::new(
            100,
            512,
            5,
            7,
            CoresDependency::Custom {
                func: |cores: u64| -> f64 {
                    if cores == 7 {
                        2.0
                    } else {
                        1.0
                    }
                },
            },
        ))),
    );
    sim.add_actor(
        "task4",
        Rc::new(RefCell::new(TaskActor::new(100, 512, 15, 20, CoresDependency::Linear))),
    );
    sim.add_actor("compute", Rc::new(RefCell::new(Compute::new(1, 10, 1024))));
    sim.add_event(Start {}, ActorId::from("0"), ActorId::from("task1"), 0.);
    sim.add_event(Start {}, ActorId::from("0"), ActorId::from("task2"), 0.);
    sim.add_event(Start {}, ActorId::from("0"), ActorId::from("task3"), 1000.);
    sim.add_event(Start {}, ActorId::from("0"), ActorId::from("task4"), 2000.);
    sim.step_until_no_events();
}
