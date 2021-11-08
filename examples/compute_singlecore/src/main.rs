use std::cell::RefCell;
use std::rc::Rc;
use std::collections::BTreeMap;

use core::sim::Simulation;
use core::actor::{Actor, ActorId, ActorContext};
use crate::Event::*;

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
pub enum Event {
    Start {
    },
    CompRequest {
        computation: Computation,
    },
    CompStarted {
        computation: Computation,
    },
    CompFinished {
        computation: Computation,
    },
    CompFailed {
        computation: Computation,
    }
}

// ACTORS //////////////////////////////////////////////////////////////////////////////////////////

pub struct TaskActor {
    task: Computation,
}

impl TaskActor {
    pub fn new(computation: Computation) -> Self {
        Self {
            task: computation,
        }
    }
}

impl Actor<Event> for TaskActor {
    fn on(&mut self, event: Event, from: ActorId, _event_id: u64, ctx: &mut ActorContext<Event>) {
        match event {
            Event::Start { } => {
                println!("{} [{}] received Start from {}", ctx.time(), ctx.id, from);
                let compute_actor = ActorId::from("compute");
                ctx.emit(CompRequest { computation: self.task.clone() }, compute_actor, 0.);
            }
            Event::CompStarted { computation } => {
                println!("{} [{}] received CompStarted from {} for {:?}", ctx.time(), ctx.id, from, computation);
            },
            Event::CompFinished { computation } => {
                println!("{} [{}] received CompFinished from {} for {:?}", ctx.time(), ctx.id, from, computation);
            },
            Event::CompFailed { computation } => {
                println!("{} [{}] received CompFailed from {} for {:?}", ctx.time(), ctx.id, from, computation);
            },
            _ => ()
        }
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

impl Actor<Event> for ComputeActor {
    fn on(&mut self, event: Event, from: ActorId, event_id: u64, ctx: &mut ActorContext<Event>) {
        match event {
            Event::CompRequest { ref computation } => {
                if self.memory < computation.memory {
                    ctx.emit(CompFailed { computation: computation.clone() }, from, 0.);
                } else {
                    println!("{} [{}] received {:?} from {}", ctx.time(), ctx.id, event, from);
                    self.memory -= computation.memory;
                    ctx.emit(CompStarted { computation: computation.clone() }, from.clone(), 0.);
                    let compute_time = computation.flops as f64 / self.speed as f64 * (self.current_computations.len() + 1) as f64;
                    let finish_event_id = ctx.emit(CompFinished { computation: computation.clone() }, ctx.id.clone(), compute_time);

                    let mut updated_computations: BTreeMap<u64, RunningComputation> = BTreeMap::new();
                    updated_computations.insert(finish_event_id, RunningComputation::new(computation.clone(), from.clone(), ctx.time(), compute_time));
                    for (&id, running_computation) in self.current_computations.iter() {
                        ctx.cancel_event(id);
                        let left_time = (running_computation.left_time - (ctx.time() - running_computation.last_update_time)) / self.current_computations.len() as f64 * (self.current_computations.len() + 1) as f64;
                        let updated_finish_event_id = ctx.emit(CompFinished { computation : running_computation.computation.clone() }, ctx.id.clone(), left_time);
                        updated_computations.insert(updated_finish_event_id, RunningComputation::new(running_computation.computation.clone(), running_computation.actor_id.clone(), ctx.time(), left_time));
                    }
                    std::mem::swap(&mut self.current_computations, &mut updated_computations);
                }
            },
            Event::CompFinished { computation } => {
                let running_computation = self.current_computations.get(&event_id).expect("Unexpected CompFinished event in ComputeActor");
                ctx.emit(CompFinished { computation: computation.clone() }, running_computation.actor_id.clone(), 0.);
                self.memory += computation.memory;
                let mut updated_computations: BTreeMap<u64, RunningComputation> = BTreeMap::new();
                for (&id, running_computation) in self.current_computations.iter() {
                    if id == event_id {
                        continue;
                    }
                    ctx.cancel_event(id);
                    let left_time = (running_computation.left_time - (ctx.time() - running_computation.last_update_time)) / self.current_computations.len() as f64 * (self.current_computations.len() - 1) as f64;
                    let updated_finish_event_id = ctx.emit(CompFinished { computation : running_computation.computation.clone() }, ctx.id.clone(), left_time);
                    updated_computations.insert(updated_finish_event_id, RunningComputation::new(running_computation.computation.clone(), running_computation.actor_id.clone(), ctx.time(), left_time));
                }
                std::mem::swap(&mut self.current_computations, &mut updated_computations);
            }
            _ => ()
        }
    }

    fn is_active(&self) -> bool {
        true
    }
}

// MAIN ////////////////////////////////////////////////////////////////////////////////////////////

fn main() {
    let mut sim = Simulation::<Event>::new(123);
    sim.add_actor("task1", Rc::new(RefCell::new(TaskActor::new(Computation::new(100, 512, 1)))));
    sim.add_actor("task2", Rc::new(RefCell::new(TaskActor::new(Computation::new(200, 512, 2)))));
    sim.add_actor("compute", Rc::new(RefCell::new(ComputeActor::new(10, 1024))));
    sim.add_event(Start {}, "0", "task1", 0.);
    sim.add_event(Start {}, "0", "task2", 5.);
    sim.step_until_no_events();
}
