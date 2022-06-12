use serde::Serialize;

use simcore::cast;
use simcore::component::Id;
use simcore::context::SimulationContext;
use simcore::event::Event;
use simcore::handler::EventHandler;

use throughput_model::fair_sharing::FairThroughputSharingModel;
use throughput_model::model::ThroughputModel;

// STRUCTS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Serialize, Clone)]
struct RunningComputation {
    id: u64,
    memory: u64,
    requester: Id,
}

impl RunningComputation {
    pub fn new(id: u64, memory: u64, requester: Id) -> Self {
        Self { id, memory, requester }
    }
}

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
struct InternalCompFinished {
    computation: RunningComputation,
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

pub struct Compute {
    #[allow(dead_code)]
    speed: u64,
    #[allow(dead_code)]
    memory_total: u64,
    memory_available: u64,
    throughput_model: FairThroughputSharingModel<RunningComputation>,
    next_event: u64,
    ctx: SimulationContext,
}

impl Compute {
    pub fn new(speed: u64, memory: u64, ctx: SimulationContext) -> Self {
        Self {
            speed,
            memory_total: memory,
            memory_available: memory,
            throughput_model: FairThroughputSharingModel::with_fixed_throughput(speed as f64),
            next_event: 0,
            ctx,
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
                    self.ctx.cancel_event(self.next_event);
                    self.throughput_model.insert(
                        self.ctx.time(),
                        flops as f64,
                        RunningComputation::new(event.id, memory, requester),
                    );
                    if let Some((time, computation)) = self.throughput_model.peek() {
                        self.next_event = self.ctx.emit_self(
                            InternalCompFinished {
                                computation: computation.clone(),
                            },
                            time - self.ctx.time(),
                        );
                    }
                }
            }
            InternalCompFinished { computation } => {
                let (_, next_computation) = self.throughput_model.pop().unwrap();
                assert!(
                    computation.id == next_computation.id,
                    "Got unexpected InternalCompFinished event"
                );
                self.memory_available += computation.memory;
                self.ctx
                    .emit_now(CompFinished { id: computation.id }, computation.requester);
                if let Some((time, computation)) = self.throughput_model.peek() {
                    self.next_event = self.ctx.emit_self(
                        InternalCompFinished {
                            computation: computation.clone(),
                        },
                        time - self.ctx.time(),
                    );
                }
            }
        })
    }
}
