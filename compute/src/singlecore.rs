use std::collections::BTreeMap;

use core::actor::{Actor, ActorContext, ActorId, Event};
use core::cast;

// EVENTS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub enum FailReason {
    NotEnoughResources { available_memory: u64 },
    Other { reason: String },
}

#[derive(Debug, Clone)]
pub struct CompRequest {
    pub flops: u64,
    pub memory: u64,
}

#[derive(Debug, Clone)]
pub struct CompStarted {
    pub id: u64,
}

#[derive(Debug, Clone)]
pub struct CompFinished {
    pub id: u64,
}

#[derive(Debug, Clone)]
pub struct CompFailed {
    pub id: u64,
    pub reason: FailReason,
}

// ACTORS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
struct RunningComputation {
    memory: u64,
    finish_event_id: u64,
    actor_id: ActorId,
    last_update_time: f64,
    left_time: f64,
}

impl RunningComputation {
    pub fn new(memory: u64, finish_event_id: u64, actor_id: ActorId, last_update_time: f64, left_time: f64) -> Self {
        Self {
            memory,
            finish_event_id,
            actor_id,
            last_update_time,
            left_time,
        }
    }
}

pub struct Compute {
    speed: u64,
    #[allow(dead_code)]
    memory_total: u64,
    memory_available: u64,
    computations: BTreeMap<u64, RunningComputation>,
}

impl Compute {
    pub fn new(speed: u64, memory: u64) -> Self {
        Self {
            speed,
            memory_total: memory,
            memory_available: memory,
            computations: BTreeMap::new(),
        }
    }
}

impl Compute {
    fn update_computation_time(&mut self, prev_size: usize, new_size: usize, ctx: &mut ActorContext) {
        for (&id, mut running_computation) in self.computations.iter_mut() {
            ctx.cancel_event(running_computation.finish_event_id);

            running_computation.left_time = (running_computation.left_time
                - (ctx.time() - running_computation.last_update_time))
                / prev_size as f64
                * new_size as f64;
            running_computation.last_update_time = ctx.time();

            running_computation.finish_event_id =
                ctx.emit(CompFinished { id }, ctx.id.clone(), running_computation.left_time);
        }
    }
}

impl Actor for Compute {
    fn on(&mut self, event: Box<dyn Event>, from: ActorId, ctx: &mut ActorContext) {
        cast!(match event {
            CompRequest { flops, memory } => {
                if self.memory_available < *memory {
                    ctx.emit(
                        CompFailed {
                            id: ctx.event_id,
                            reason: FailReason::NotEnoughResources {
                                available_memory: self.memory_available,
                            },
                        },
                        from.clone(),
                        0.,
                    );
                } else {
                    self.memory_available -= memory;
                    ctx.emit(CompStarted { id: ctx.event_id }, from.clone(), 0.);
                    let compute_time = *flops as f64 / self.speed as f64 * (self.computations.len() + 1) as f64;
                    let finish_event_id = ctx.emit(CompFinished { id: ctx.event_id }, ctx.id.clone(), compute_time);

                    self.update_computation_time(self.computations.len(), self.computations.len() + 1, ctx);

                    self.computations.insert(
                        ctx.event_id,
                        RunningComputation::new(*memory, finish_event_id, from.clone(), ctx.time(), compute_time),
                    );
                }
            }
            CompFinished { id } => {
                let running_computation = self
                    .computations
                    .get(&id)
                    .expect("Unexpected CompFinished event in Compute");
                ctx.emit(CompFinished { id: *id }, running_computation.actor_id.clone(), 0.);
                self.memory_available += running_computation.memory;

                self.computations.remove(id).unwrap();
                self.update_computation_time(self.computations.len() + 1, self.computations.len(), ctx);
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}
