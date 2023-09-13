//! Actor representing computing resource with a single core.

use serde::Serialize;

use dslab_core::cast;
use dslab_core::component::Id;
use dslab_core::context::SimulationContext;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;

use dslab_models::throughput_sharing::{FairThroughputSharingModel, ThroughputSharingModel};

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

/// Reason for failure.
#[derive(Clone, Debug, Serialize)]
pub enum FailReason {
    /// Resource doesn't have enough memory.
    NotEnoughResources {
        /// Amount of currently available memory.
        available_memory: u64,
    },
}

/// Event to start a computation.
#[derive(Clone, Serialize)]
pub struct CompRequest {
    /// Total computation size.
    pub flops: f64,
    /// Total memory needed for a computation.
    pub memory: u64,
    /// Id of actor to notify about events corresponding to this computation.
    pub requester: Id,
}

/// Event corresponding to successfully started computation.
#[derive(Clone, Serialize)]
pub struct CompStarted {
    /// Id of the computation.
    pub id: u64,
}

#[derive(Clone, Serialize)]
struct InternalCompFinished {
    computation: RunningComputation,
}

/// Event corresponding to successfully finished computation.
#[derive(Clone, Serialize)]
pub struct CompFinished {
    /// Id of the computation.
    pub id: u64,
}

/// Event corresponding to failed computation.
#[derive(Clone, Serialize)]
pub struct CompFailed {
    /// Id of the computation.
    pub id: u64,
    /// Reason for failure.
    pub reason: FailReason,
}

// ACTORS //////////////////////////////////////////////////////////////////////////////////////////

/// Represents compute actor with fixed memory and one core supporting arbitrary number of parallel tasks.
pub struct Compute {
    #[allow(dead_code)]
    speed: f64,
    #[allow(dead_code)]
    memory_total: u64,
    memory_available: u64,
    throughput_model: FairThroughputSharingModel<RunningComputation>,
    next_event: u64,
    ctx: SimulationContext,
}

impl Compute {
    /// Creates new compute actor.
    pub fn new(speed: f64, memory: u64, ctx: SimulationContext) -> Self {
        Self {
            speed,
            memory_total: memory,
            memory_available: memory,
            throughput_model: FairThroughputSharingModel::with_fixed_throughput(speed),
            next_event: 0,
            ctx,
        }
    }

    /// Starts computation with given parameters and returns computation id.
    pub fn run(&mut self, flops: f64, memory: u64, requester: Id) -> u64 {
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
                        RunningComputation::new(event.id, memory, requester),
                        flops,
                        &mut self.ctx,
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
