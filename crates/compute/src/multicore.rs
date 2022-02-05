use std::collections::HashMap;

use core::actor::{Actor, ActorContext, ActorId, Event};
use core::cast;

// STRUCTS /////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct Allocation {
    pub cores: u32,
    pub memory: u64,
}

impl Allocation {
    pub fn new(cores: u32, memory: u64) -> Self {
        Self { cores, memory }
    }
}

// [1 .. max_cores] -> [1, +inf]
#[derive(Debug, Clone, Copy)]
pub enum CoresDependency {
    Linear,
    LinearWithFixed { fixed_part: f64 },
    Custom { func: fn(u32) -> f64 },
}

#[derive(Debug, Clone)]
pub enum FailReason {
    NotEnoughResources {
        available_cores: u32,
        available_memory: u64,
    },
    Other {
        reason: String,
    },
}

#[derive(Debug)]
struct RunningComputation {
    cores: u32,
    memory: u64,
    actor_id: ActorId,
}

impl RunningComputation {
    fn new(cores: u32, memory: u64, actor_id: ActorId) -> Self {
        RunningComputation {
            cores,
            memory,
            actor_id,
        }
    }
}

// EVENTS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct CompRequest {
    pub flops: u64,
    pub memory: u64,
    pub min_cores: u32,
    pub max_cores: u32,
    pub cores_dependency: CoresDependency,
    pub requester: ActorId,
}

#[derive(Debug, Clone)]
pub struct CompStarted {
    pub id: u64,
    pub cores: u32,
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

#[derive(Debug, Clone)]
pub struct AllocationRequest {
    pub allocation: Allocation,
    pub requester: ActorId,
}

#[derive(Debug, Clone)]
pub struct AllocationSuccess {
    pub id: u64,
}

#[derive(Debug, Clone)]
pub struct AllocationFailed {
    pub id: u64,
    pub reason: FailReason,
}

#[derive(Debug, Clone)]
pub struct DeallocationRequest {
    pub allocation: Allocation,
    pub requester: ActorId,
}

#[derive(Debug, Clone)]
pub struct DeallocationSuccess {
    pub id: u64,
}

#[derive(Debug, Clone)]
pub struct DeallocationFailed {
    pub id: u64,
    pub reason: FailReason,
}

// ACTORS //////////////////////////////////////////////////////////////////////////////////////////

pub struct Compute {
    id: ActorId,
    speed: u64,
    cores_total: u32,
    cores_available: u32,
    memory_total: u64,
    memory_available: u64,
    computations: HashMap<u64, RunningComputation>,
    allocations: HashMap<ActorId, Allocation>,
}

impl Compute {
    pub fn new(id: &str, speed: u64, cores: u32, memory: u64) -> Self {
        Self {
            id: ActorId::from(id),
            speed,
            cores_total: cores,
            cores_available: cores,
            memory_total: memory,
            memory_available: memory,
            computations: HashMap::new(),
            allocations: HashMap::new(),
        }
    }

    pub fn speed(&self) -> u64 {
        self.speed
    }

    pub fn cores_total(&self) -> u32 {
        self.cores_total
    }

    pub fn cores_available(&self) -> u32 {
        self.cores_available
    }

    pub fn memory_total(&self) -> u64 {
        self.memory_total
    }

    pub fn memory_available(&self) -> u64 {
        self.memory_available
    }

    pub fn run(
        &self,
        flops: u64,
        memory: u64,
        min_cores: u32,
        max_cores: u32,
        cores_dependency: CoresDependency,
        ctx: &mut ActorContext,
    ) -> u64 {
        let request = CompRequest {
            flops,
            memory,
            min_cores,
            max_cores,
            cores_dependency,
            requester: ctx.id.clone(),
        };
        ctx.emit_now(request, self.id.clone())
    }

    pub fn allocate(&self, cores: u32, memory: u64, ctx: &mut ActorContext) -> u64 {
        let request = AllocationRequest {
            allocation: Allocation::new(cores, memory),
            requester: ctx.id.clone(),
        };
        ctx.emit_now(request, self.id.clone())
    }

    pub fn deallocate(&self, cores: u32, memory: u64, ctx: &mut ActorContext) -> u64 {
        let request = DeallocationRequest {
            allocation: Allocation::new(cores, memory),
            requester: ctx.id.clone(),
        };
        ctx.emit_now(request, self.id.clone())
    }
}

impl Actor for Compute {
    fn on(&mut self, event: Box<dyn Event>, _from: ActorId, ctx: &mut ActorContext) {
        cast!(match event {
            CompRequest {
                flops,
                memory,
                min_cores,
                max_cores,
                ref cores_dependency,
                requester,
            } => {
                if self.memory_available < *memory || self.cores_available < *min_cores {
                    ctx.emit_now(
                        CompFailed {
                            id: ctx.event_id,
                            reason: FailReason::NotEnoughResources {
                                available_cores: self.cores_available,
                                available_memory: self.memory_available,
                            },
                        },
                        requester.clone(),
                    );
                } else {
                    let cores = self.cores_available.min(*max_cores);
                    self.memory_available -= *memory;
                    self.cores_available -= cores;
                    ctx.emit_now(
                        CompStarted {
                            id: ctx.event_id,
                            cores,
                        },
                        requester.clone(),
                    );

                    let speedup = match cores_dependency {
                        CoresDependency::Linear => cores as f64,
                        CoresDependency::LinearWithFixed { fixed_part } => {
                            1. / (fixed_part + (1. - fixed_part) / cores as f64)
                        }
                        CoresDependency::Custom { func } => func(cores),
                    };

                    let compute_time = *flops as f64 / self.speed as f64 / speedup;
                    ctx.emit(CompFinished { id: ctx.event_id }, ctx.id.clone(), compute_time);
                    self.computations
                        .insert(ctx.event_id, RunningComputation::new(cores, *memory, requester.clone()));
                }
            }
            CompFinished { id } => {
                let running_computation = self
                    .computations
                    .remove(&id)
                    .expect("Unexpected CompFinished event in Compute");
                self.memory_available += running_computation.memory;
                self.cores_available += running_computation.cores;
                ctx.emit(CompFinished { id: *id }, running_computation.actor_id, 0.);
            }
            AllocationRequest { allocation, requester } => {
                if self.memory_available < allocation.memory || self.cores_available < allocation.cores {
                    ctx.emit_now(
                        AllocationFailed {
                            id: ctx.event_id,
                            reason: FailReason::NotEnoughResources {
                                available_cores: self.cores_available,
                                available_memory: self.memory_available,
                            },
                        },
                        requester.clone(),
                    );
                } else {
                    let current_allocation = self
                        .allocations
                        .entry(requester.clone())
                        .or_insert(Allocation::new(0, 0));
                    current_allocation.cores += allocation.cores;
                    current_allocation.memory += allocation.memory;
                    self.cores_available -= allocation.cores;
                    self.memory_available -= allocation.memory;
                    ctx.emit(AllocationSuccess { id: ctx.event_id }, requester.clone(), 0.);
                }
            }
            DeallocationRequest { allocation, requester } => {
                let current_allocation = self
                    .allocations
                    .entry(requester.clone())
                    .or_insert(Allocation::new(0, 0));
                if current_allocation.cores >= allocation.cores && current_allocation.memory >= allocation.memory {
                    current_allocation.cores -= allocation.cores;
                    current_allocation.memory -= allocation.memory;
                    self.cores_available += allocation.cores;
                    self.memory_available += allocation.memory;
                    ctx.emit(DeallocationSuccess { id: ctx.event_id }, requester.clone(), 0.);
                } else {
                    ctx.emit_now(
                        DeallocationFailed {
                            id: ctx.event_id,
                            reason: FailReason::NotEnoughResources {
                                available_cores: current_allocation.cores,
                                available_memory: current_allocation.memory,
                            },
                        },
                        requester.clone(),
                    );
                }
                if current_allocation.cores == 0 && current_allocation.memory == 0 {
                    self.allocations.remove(&requester);
                }
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}
