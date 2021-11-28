use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use core::actor::{Actor, ActorContext, ActorId, Event};
use core::match_event;
use core::sim::Simulation;

// EVENTS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct Computation {
    flops: u64,
    memory: u64,
    id: u64,
}

impl Computation {
    pub fn new(flops: u64, memory: u64, id: u64) -> Self {
        Self {
            flops: flops,
            memory: memory,
            id: id,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Start {}

#[derive(Debug, Clone)]
pub struct CompRequest {
    computation: Computation,
}

#[derive(Debug, Clone)]
pub struct CompStarted {
    computation: Computation,
}

#[derive(Debug, Clone)]
pub struct CompFinished {
    computation: Computation,
}

#[derive(Debug, Clone)]
pub struct CompFailed {
    computation: Computation,
}

// ACTORS //////////////////////////////////////////////////////////////////////////////////////////

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
            CompFailed { computation } => {
                println!(
                    "{} [{}] received CompFailed from {} for {:?}",
                    ctx.time(),
                    ctx.id,
                    from,
                    computation
                );
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}

pub struct RunningComputation {
    computation: Computation,
    actor_id: ActorId,
    last_update_time: f64,
    left_time: f64,
}

impl RunningComputation {
    pub fn new(computation: Computation, actor_id: ActorId, last_update_time: f64, left_time: f64) -> Self {
        Self {
            computation: computation,
            actor_id: actor_id,
            last_update_time: last_update_time,
            left_time: left_time,
        }
    }
}

pub struct ComputeActor {
    speed: u64,
    memory: u64,
    current_computations: BTreeMap<u64, RunningComputation>,
}

impl ComputeActor {
    pub fn new(speed: u64, memory: u64) -> Self {
        Self {
            speed: speed,
            memory: memory,
            current_computations: BTreeMap::new(),
        }
    }
}

impl Actor for ComputeActor {
    fn on(&mut self, event: Box<dyn Event>, from: ActorId, ctx: &mut ActorContext) {
        match_event!( event {
            CompRequest { ref computation } => {
                if self.memory < computation.memory {
                    ctx.emit(
                        CompFailed {
                            computation: computation.clone(),
                        },
                        from.clone(),
                        0.,
                    );
                } else {
                    println!("{} [{}] received {:?} from {}", ctx.time(), ctx.id, event, from);
                    self.memory -= computation.memory;
                    ctx.emit(
                        CompStarted {
                            computation: computation.clone(),
                        },
                        from.clone(),
                        0.,
                    );
                    let compute_time =
                        computation.flops as f64 / self.speed as f64 * (self.current_computations.len() + 1) as f64;
                    let finish_event_id = ctx.emit(
                        CompFinished {
                            computation: computation.clone(),
                        },
                        ctx.id.clone(),
                        compute_time,
                    );

                    let mut updated_computations: BTreeMap<u64, RunningComputation> = BTreeMap::new();
                    updated_computations.insert(
                        finish_event_id,
                        RunningComputation::new(computation.clone(), from.clone(), ctx.time(), compute_time),
                    );
                    for (&id, running_computation) in self.current_computations.iter() {
                        ctx.cancel_event(id);
                        let left_time = (running_computation.left_time
                            - (ctx.time() - running_computation.last_update_time))
                            / self.current_computations.len() as f64
                            * (self.current_computations.len() + 1) as f64;
                        let updated_finish_event_id = ctx.emit(
                            CompFinished {
                                computation: running_computation.computation.clone(),
                            },
                            ctx.id.clone(),
                            left_time,
                        );
                        updated_computations.insert(
                            updated_finish_event_id,
                            RunningComputation::new(
                                running_computation.computation.clone(),
                                running_computation.actor_id.clone(),
                                ctx.time(),
                                left_time,
                            ),
                        );
                    }
                    std::mem::swap(&mut self.current_computations, &mut updated_computations);
                }
            },
            CompFinished { computation } => {
                let running_computation = self
                    .current_computations
                    .get(&ctx.event_id)
                    .expect("Unexpected CompFinished event in ComputeActor");
                ctx.emit(
                    CompFinished {
                        computation: computation.clone(),
                    },
                    running_computation.actor_id.clone(),
                    0.,
                );
                self.memory += computation.memory;
                let mut updated_computations: BTreeMap<u64, RunningComputation> = BTreeMap::new();
                for (&id, running_computation) in self.current_computations.iter() {
                    if id == ctx.event_id {
                        continue;
                    }
                    ctx.cancel_event(id);
                    let left_time = (running_computation.left_time
                        - (ctx.time() - running_computation.last_update_time))
                        / self.current_computations.len() as f64
                        * (self.current_computations.len() - 1) as f64;
                    let updated_finish_event_id = ctx.emit(
                        CompFinished {
                            computation: running_computation.computation.clone(),
                        },
                        ctx.id.clone(),
                        left_time,
                    );
                    updated_computations.insert(
                        updated_finish_event_id,
                        RunningComputation::new(
                            running_computation.computation.clone(),
                            running_computation.actor_id.clone(),
                            ctx.time(),
                            left_time,
                        ),
                    );
                }
                std::mem::swap(&mut self.current_computations, &mut updated_computations);
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}

// MAIN ////////////////////////////////////////////////////////////////////////////////////////////

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
