use std::collections::BTreeMap;

use crate::computation::*;
use core::actor::{Actor, ActorContext, ActorId, Event};
use core::match_event;

// EVENTS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub enum FailReason {
    NotEnoughResources { available_memory: u64 },
    Other { reason: String },
}

#[derive(Debug, Clone)]
pub struct CompRequest {
    pub computation: Computation,
}

#[derive(Debug, Clone)]
pub struct CompStarted {
    pub computation: Computation,
}

#[derive(Debug, Clone)]
pub struct CompFinished {
    pub computation: Computation,
}

#[derive(Debug, Clone)]
pub struct CompFailed {
    pub computation: Computation,
    pub reason: FailReason,
}

// ACTORS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
struct RunningComputation {
    computation: Computation,
    actor_id: ActorId,
    last_update_time: f64,
    left_time: f64,
}

impl RunningComputation {
    pub fn new(computation: Computation, actor_id: ActorId, last_update_time: f64, left_time: f64) -> Self {
        Self {
            computation,
            actor_id,
            last_update_time,
            left_time,
        }
    }
}

pub struct ComputeActor {
    speed: u64,
    #[allow(dead_code)]
    memory_total: u64,
    memory_available: u64,
    computations: BTreeMap<u64, RunningComputation>,
}

impl ComputeActor {
    pub fn new(speed: u64, memory: u64) -> Self {
        Self {
            speed,
            memory_total: memory,
            memory_available: memory,
            computations: BTreeMap::new(),
        }
    }
}

impl Actor for ComputeActor {
    fn on(&mut self, event: Box<dyn Event>, from: ActorId, ctx: &mut ActorContext) {
        match_event!( event {
            CompRequest { computation } => {
                if self.memory_available < computation.memory {
                    ctx.emit(
                        CompFailed {
                            computation: computation.clone(),
                            reason: FailReason::NotEnoughResources {
                                available_memory: self.memory_available,
                            },
                        },
                        from.clone(),
                        0.,
                    );
                } else {
                    println!("{} [{}] received {:?} from {}", ctx.time(), ctx.id, event, from);
                    self.memory_available -= computation.memory;
                    ctx.emit(
                        CompStarted {
                            computation: computation.clone(),
                        },
                        from.clone(),
                        0.,
                    );
                    let compute_time =
                        computation.flops as f64 / self.speed as f64 * (self.computations.len() + 1) as f64;
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
                    for (&id, running_computation) in self.computations.iter() {
                        ctx.cancel_event(id);

                        let mut computation = running_computation.clone();
                        computation.left_time = (computation.left_time - (ctx.time() - computation.last_update_time))
                            / self.computations.len() as f64
                            * (self.computations.len() + 1) as f64;
                        computation.last_update_time = ctx.time();

                        let updated_finish_event_id = ctx.emit(
                            CompFinished {
                                computation: computation.computation.clone(),
                            },
                            ctx.id.clone(),
                            computation.left_time,
                        );

                        updated_computations.insert(updated_finish_event_id, computation);
                    }
                    std::mem::swap(&mut self.computations, &mut updated_computations);
                }
            },
            CompFinished { computation } => {
                let running_computation = self
                    .computations
                    .get(&ctx.event_id)
                    .expect("Unexpected CompFinished event in ComputeActor");
                ctx.emit(
                    CompFinished {
                        computation: computation.clone(),
                    },
                    running_computation.actor_id.clone(),
                    0.,
                );
                self.memory_available += computation.memory;
                let mut updated_computations: BTreeMap<u64, RunningComputation> = BTreeMap::new();
                for (&id, running_computation) in self.computations.iter() {
                    if id == ctx.event_id {
                        continue;
                    }
                    ctx.cancel_event(id);

                    let mut computation = running_computation.clone();
                    computation.left_time = (computation.left_time - (ctx.time() - computation.last_update_time))
                        / self.computations.len() as f64
                        * (self.computations.len() - 1) as f64;
                    computation.last_update_time = ctx.time();

                    let updated_finish_event_id = ctx.emit(
                        CompFinished {
                            computation: computation.computation.clone(),
                        },
                        ctx.id.clone(),
                        computation.left_time,
                    );

                    updated_computations.insert(updated_finish_event_id, computation);
                }
                std::mem::swap(&mut self.computations, &mut updated_computations);
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}
