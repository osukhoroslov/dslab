use std::cell::RefCell;
use std::rc::Rc;

use compute::computation::Computation;
use compute::multicore::*;
use core::actor::{Actor, ActorContext, ActorId, Event};
use core::match_event;
use core::sim::Simulation;

pub struct TaskActor {
    task: Computation,
    min_cores: u64,
    max_cores: u64,
    cores_dependency: CoresDependency,
}

impl TaskActor {
    pub fn new(computation: Computation, min_cores: u64, max_cores: u64, cores_dependency: CoresDependency) -> Self {
        Self {
            task: computation,
            min_cores: min_cores,
            max_cores: max_cores,
            cores_dependency: cores_dependency,
        }
    }
}

impl Actor for TaskActor {
    fn on(&mut self, event: Box<dyn Event>, from: ActorId, ctx: &mut ActorContext) {
        match_event!( event {
            Start {} => {
                println!("{} [{}] received Start from {}", ctx.time(), ctx.id, from);
                let compute_actor = ActorId::from("compute");
                ctx.emit(
                    CompRequest {
                        computation: self.task.clone(),
                        min_cores: self.min_cores,
                        max_cores: self.max_cores,
                        cores_dependency: self.cores_dependency.clone(),
                    },
                    compute_actor,
                    0.,
                );
            },
            CompStarted { computation, cores } => {
                println!(
                    "{} [{}] received CompStarted from {} for {:?} on {} cores",
                    ctx.time(),
                    ctx.id,
                    from,
                    computation,
                    cores
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
                    "{} [{}] received CompFailed from {} for {:?} with reason {:?}",
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
        match_event!( event {
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
            },
            AllocationFailed { allocation, reason } => {
                println!(
                    "{} [{}] received AllocationFailed from {} for {:?} with reason {:?}",
                    ctx.time(),
                    ctx.id,
                    from,
                    allocation,
                    reason
                );
            },
            DeallocationFailed { allocation, reason } => {
                println!(
                    "{} [{}] received DeallocationFailed from {} for {:?} with reason {:?}",
                    ctx.time(),
                    ctx.id,
                    from,
                    allocation,
                    reason
                );
            },
            AllocationSuccess { allocation } => {
                println!(
                    "{} [{}] received AllocationSuccess from {} for {:?}",
                    ctx.time(),
                    ctx.id,
                    from,
                    allocation
                );
            },
            DeallocationSuccess { allocation } => {
                println!(
                    "{} [{}] received DeallocationSuccess from {} for {:?}",
                    ctx.time(),
                    ctx.id,
                    from,
                    allocation
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
        Rc::new(RefCell::new(TaskActor::new(
            Computation::new(100, 512, 1),
            2,
            6,
            CoresDependency::Linear,
        ))),
    );
    sim.add_actor(
        "task2",
        Rc::new(RefCell::new(TaskActor::new(
            Computation::new(100, 512, 2),
            4,
            10,
            CoresDependency::LinearWithFixed { fixed_part: 0.4 },
        ))),
    );
    sim.add_actor(
        "task3",
        Rc::new(RefCell::new(TaskActor::new(
            Computation::new(100, 512, 3),
            5,
            7,
            CoresDependency::Custom {
                func: |cores: u64| -> f64 {
                    if cores == 7 {
                        0.5
                    } else {
                        1.0
                    }
                },
            },
        ))),
    );
    sim.add_actor(
        "task4",
        Rc::new(RefCell::new(TaskActor::new(
            Computation::new(100, 512, 4),
            15,
            20,
            CoresDependency::Linear,
        ))),
    );
    sim.add_actor("compute", Rc::new(RefCell::new(ComputeActor::new(1, 10, 1024))));
    sim.add_event(Start {}, ActorId::from("0"), ActorId::from("task1"), 0.);
    sim.add_event(Start {}, ActorId::from("0"), ActorId::from("task2"), 0.);
    sim.add_event(Start {}, ActorId::from("0"), ActorId::from("task3"), 1000.);
    sim.add_event(Start {}, ActorId::from("0"), ActorId::from("task4"), 2000.);
    sim.step_until_no_events();
}
