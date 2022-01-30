use std::cell::RefCell;
use std::rc::Rc;

use compute::multicore::*;
use core::actor::{Actor, ActorContext, ActorId, Event};
use core::cast;
use core::sim::Simulation;

#[derive(Debug, Clone)]
pub struct Start {}

#[derive(Debug, Clone)]
pub struct Deallocate {}

pub struct TaskActor {
    compute: Rc<RefCell<Compute>>,
    flops: u64,
    memory: u64,
    min_cores: u32,
    max_cores: u32,
    cores_dependency: CoresDependency,
}

impl TaskActor {
    pub fn new(
        compute: Rc<RefCell<Compute>>,
        flops: u64,
        memory: u64,
        min_cores: u32,
        max_cores: u32,
        cores_dependency: CoresDependency,
    ) -> Self {
        Self {
            compute,
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
                self.compute.borrow().run(
                    self.flops,
                    self.memory,
                    self.min_cores,
                    self.max_cores,
                    self.cores_dependency.clone(),
                    ctx,
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
    compute: Rc<RefCell<Compute>>,
    allocation: Allocation,
    time: f64,
}

impl AllocationActor {
    pub fn new(compute: Rc<RefCell<Compute>>, allocation: Allocation, time: f64) -> Self {
        Self {
            compute,
            allocation,
            time,
        }
    }
}

impl Actor for AllocationActor {
    fn on(&mut self, event: Box<dyn Event>, from: ActorId, ctx: &mut ActorContext) {
        cast!(match event {
            Start {} => {
                println!("{} [{}] received Start from {}", ctx.time(), ctx.id, from);
                self.compute
                    .borrow()
                    .allocate(self.allocation.cores, self.allocation.memory, ctx);
                ctx.emit(Deallocate {}, ctx.id.clone(), self.time);
            }
            Deallocate {} => {
                self.compute
                    .borrow()
                    .deallocate(self.allocation.cores, self.allocation.memory, ctx);
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
    let compute = Rc::new(RefCell::new(Compute::new("compute", 1, 10, 1024)));
    sim.add_actor(
        "task1",
        Rc::new(RefCell::new(TaskActor::new(
            compute.clone(),
            100,
            512,
            2,
            6,
            CoresDependency::Linear,
        ))),
    );
    sim.add_actor(
        "task2",
        Rc::new(RefCell::new(TaskActor::new(
            compute.clone(),
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
            compute.clone(),
            100,
            512,
            5,
            7,
            CoresDependency::Custom {
                func: |cores: u32| -> f64 {
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
        Rc::new(RefCell::new(TaskActor::new(
            compute.clone(),
            100,
            512,
            15,
            20,
            CoresDependency::Linear,
        ))),
    );
    sim.add_actor(
        "allocate1",
        Rc::new(RefCell::new(AllocationActor::new(
            compute.clone(),
            Allocation::new(6, 100),
            10.,
        ))),
    );
    sim.add_actor(
        "allocate2",
        Rc::new(RefCell::new(AllocationActor::new(
            compute.clone(),
            Allocation::new(6, 100),
            20.,
        ))),
    );
    sim.add_actor(
        "allocate3",
        Rc::new(RefCell::new(AllocationActor::new(
            compute.clone(),
            Allocation::new(6, 100),
            30.,
        ))),
    );
    sim.add_actor("compute", compute);
    sim.add_event(Start {}, ActorId::from("0"), ActorId::from("task1"), 0.);
    sim.add_event(Start {}, ActorId::from("0"), ActorId::from("task2"), 0.);
    sim.add_event(Start {}, ActorId::from("0"), ActorId::from("task3"), 1000.);
    sim.add_event(Start {}, ActorId::from("0"), ActorId::from("task4"), 2000.);
    sim.add_event(Start {}, ActorId::from("0"), ActorId::from("allocate1"), 5000.);
    sim.add_event(Start {}, ActorId::from("0"), ActorId::from("allocate2"), 5005.);
    sim.add_event(Start {}, ActorId::from("0"), ActorId::from("allocate3"), 6000.);
    sim.step_until_no_events();
}
