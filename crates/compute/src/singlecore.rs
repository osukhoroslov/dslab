use std::collections::BTreeMap;

use serde::Serialize;

use simcore::cast;
use simcore::component::Id;
use simcore::context::SimulationContext;
use simcore::event::Event;
use simcore::handler::EventHandler;

// EVENTS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Serialize, Debug)]
pub enum FailReason {
    NotEnoughResources { available_memory: u64 },
    Other { reason: String },
}

#[derive(Serialize)]
pub struct CompRequest {
    pub flops: u64,
    pub memory: u64,
    pub requester: Id,
}

#[derive(Serialize)]
pub struct CompStarted {
    pub id: u64,
}

#[derive(Serialize)]
pub struct CompFinished {
    pub id: u64,
}

#[derive(Serialize)]
pub struct CompFailed {
    pub id: u64,
    pub reason: FailReason,
}

// ACTORS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
struct RunningComputation {
    memory: u64,
    finish_event_id: u64,
    requester: Id,
    last_update_time: f64,
    left_time: f64,
}

impl RunningComputation {
    pub fn new(memory: u64, finish_event_id: u64, requester: Id, last_update_time: f64, left_time: f64) -> Self {
        Self {
            memory,
            finish_event_id,
            requester,
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
    ctx: SimulationContext,
}

impl Compute {
    pub fn new(speed: u64, memory: u64, ctx: SimulationContext) -> Self {
        Self {
            speed,
            memory_total: memory,
            memory_available: memory,
            computations: BTreeMap::new(),
            ctx,
        }
    }

    fn update_computation_time(&mut self, prev_size: usize, new_size: usize) {
        for (&id, mut running_computation) in self.computations.iter_mut() {
            self.ctx.cancel_event(running_computation.finish_event_id);

            running_computation.left_time = (running_computation.left_time
                - (self.ctx.time() - running_computation.last_update_time))
                / prev_size as f64
                * new_size as f64;
            running_computation.last_update_time = self.ctx.time();

            running_computation.finish_event_id =
                self.ctx.emit_self(CompFinished { id }, running_computation.left_time);
        }
    }

    pub fn run(&mut self, flops: u64, memory: u64, requester: Id) -> u64 {
        let request = CompRequest {
            flops,
            memory,
            requester,
        };
        self.ctx.emit_self_now(request)
    }
}

impl EventHandler for Compute {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            CompRequest {
                flops,
                memory,
                requester,
            } => {
                if self.memory_available < memory {
                    self.ctx.emit_now(
                        CompFailed {
                            id: event.id,
                            reason: FailReason::NotEnoughResources {
                                available_memory: self.memory_available,
                            },
                        },
                        requester,
                    );
                } else {
                    self.memory_available -= memory;
                    self.ctx.emit(CompStarted { id: event.id }, requester, 0.);
                    let compute_time = flops as f64 / self.speed as f64 * (self.computations.len() + 1) as f64;
                    let finish_event_id = self.ctx.emit_self(CompFinished { id: event.id }, compute_time);

                    self.update_computation_time(self.computations.len(), self.computations.len() + 1);

                    self.computations.insert(
                        event.id,
                        RunningComputation::new(memory, finish_event_id, requester, self.ctx.time(), compute_time),
                    );
                }
            }
            CompFinished { id } => {
                let running_computation = self
                    .computations
                    .get(&id)
                    .expect("Unexpected CompFinished event in Compute");
                self.ctx.emit_now(CompFinished { id }, running_computation.requester);
                self.memory_available += running_computation.memory;

                self.computations.remove(&id).unwrap();
                self.update_computation_time(self.computations.len() + 1, self.computations.len());
            }
        })
    }
}
