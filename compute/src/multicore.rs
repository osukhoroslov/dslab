use std::collections::HashMap;

use crate::computation::*;
use core::actor::{Actor, ActorContext, ActorId, Event};
use core::match_event;

// STRUCTS /////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct Allocation {
    cores: u64,
    memory: u64,
}

impl Allocation {
    pub fn new(cores: u64, memory: u64) -> Self {
        Self { cores, memory }
    }
}

// [1 .. max_cores] -> [1, +inf]
#[derive(Debug, Clone)]
pub enum CoresDependency {
    Linear,
    LinearWithFixed { fixed_part: f64 },
    Custom { func: fn(u64) -> f64 },
}

#[derive(Debug, Clone)]
pub enum FailReason {
    NotEnoughResources {
        available_cores: u64,
        available_memory: u64,
    },
    Other {
        reason: String,
    },
}

// EVENTS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct CompRequest {
    pub computation: Computation,
    pub min_cores: u64,
    pub max_cores: u64,
    pub cores_dependency: CoresDependency,
}

#[derive(Debug, Clone)]
pub struct CompStarted {
    pub computation: Computation,
    pub cores: u64,
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

#[derive(Debug, Clone)]
pub struct AllocationRequest {
    pub allocation: Allocation,
}

#[derive(Debug, Clone)]
pub struct AllocationSuccess {
    pub allocation: Allocation,
}

#[derive(Debug, Clone)]
pub struct AllocationFailed {
    pub allocation: Allocation,
    pub reason: FailReason,
}

#[derive(Debug, Clone)]
pub struct DeallocationRequest {
    pub allocation: Allocation,
}

#[derive(Debug, Clone)]
pub struct DeallocationSuccess {
    pub allocation: Allocation,
}

#[derive(Debug, Clone)]
pub struct DeallocationFailed {
    pub allocation: Allocation,
    pub reason: FailReason,
}

// ACTORS //////////////////////////////////////////////////////////////////////////////////////////

pub struct ComputeActor {
    speed: u64,
    #[allow(dead_code)]
    cores_total: u64,
    cores_available: u64,
    #[allow(dead_code)]
    memory_total: u64,
    memory_available: u64,
    computations: HashMap<u64, (u64, ActorId)>,
    allocations: HashMap<ActorId, Allocation>,
}

impl ComputeActor {
    pub fn new(speed: u64, cores: u64, memory: u64) -> Self {
        Self {
            speed,
            cores_total: cores,
            cores_available: cores,
            memory_total: memory,
            memory_available: memory,
            computations: HashMap::new(),
            allocations: HashMap::new(),
        }
    }
}

impl Actor for ComputeActor {
    fn on(&mut self, event: Box<dyn Event>, from: ActorId, ctx: &mut ActorContext) {
        println!("{} [{}] received {:?} from {}", ctx.time(), ctx.id, event, from);
        match_event!( event {
            CompRequest {
                ref computation,
                min_cores,
                max_cores,
                ref cores_dependency,
            } => {
                if self.memory_available < computation.memory || self.cores_available < *min_cores {
                    ctx.emit(
                        CompFailed {
                            computation: computation.clone(),
                            reason: FailReason::NotEnoughResources {
                                available_cores: self.cores_available,
                                available_memory: self.memory_available,
                            },
                        },
                        from.clone(),
                        0.,
                    );
                } else {
                    let cores = self.cores_available.min(*max_cores);
                    self.memory_available -= computation.memory;
                    self.cores_available -= cores;
                    ctx.emit(
                        CompStarted {
                            computation: computation.clone(),
                            cores,
                        },
                        from.clone(),
                        0.,
                    );

                    let speedup = match cores_dependency {
                        CoresDependency::Linear => cores as f64,
                        CoresDependency::LinearWithFixed { fixed_part } => {
                            1. / (fixed_part + (1. - fixed_part) / cores as f64)
                        }
                        CoresDependency::Custom { func } => func(cores),
                    };

                    let compute_time = computation.flops as f64 / self.speed as f64 / speedup;
                    let finish_event_id = ctx.emit(
                        CompFinished {
                            computation: computation.clone(),
                        },
                        ctx.id.clone(),
                        compute_time,
                    );
                    self.computations.insert(finish_event_id, (cores, from.clone()));
                }
            },
            CompFinished { computation } => {
                let (cores, actor_id) = self
                    .computations
                    .remove(&ctx.event_id)
                    .expect("Unexpected CompFinished event in ComputeActor");
                self.memory_available += computation.memory;
                self.cores_available += cores;
                ctx.emit(
                    CompFinished {
                        computation: computation.clone(),
                    },
                    actor_id,
                    0.,
                );
            },
            AllocationRequest { allocation } => {
                if self.memory_available < allocation.memory || self.cores_available < allocation.cores {
                    ctx.emit(
                        AllocationFailed {
                            allocation: allocation.clone(),
                            reason: FailReason::NotEnoughResources {
                                available_cores: self.cores_available,
                                available_memory: self.memory_available,
                            },
                        },
                        from.clone(),
                        0.,
                    );
                } else {
                    let current_allocation = self.allocations.entry(from.clone()).or_insert(Allocation::new(0, 0));
                    current_allocation.cores += allocation.cores;
                    current_allocation.memory += allocation.memory;
                    self.cores_available -= allocation.cores;
                    self.memory_available -= allocation.memory;
                    ctx.emit(
                        AllocationSuccess {
                            allocation: allocation.clone(),
                        },
                        from.clone(),
                        0.,
                    );
                }
            },
            DeallocationRequest { allocation } => {
                let current_allocation = self.allocations.entry(from.clone()).or_insert(Allocation::new(0, 0));
                if current_allocation.cores >= allocation.cores && current_allocation.memory >= allocation.memory {
                    current_allocation.cores -= allocation.cores;
                    current_allocation.memory -= allocation.memory;
                    self.cores_available += allocation.cores;
                    self.memory_available += allocation.memory;
                    ctx.emit(
                        DeallocationSuccess {
                            allocation: allocation.clone(),
                        },
                        from.clone(),
                        0.,
                    );
                } else {
                    ctx.emit(
                        DeallocationFailed {
                            allocation: allocation.clone(),
                            reason: FailReason::NotEnoughResources {
                                available_cores: self.cores_available,
                                available_memory: self.memory_available,
                            },
                        },
                        from.clone(),
                        0.,
                    );
                }
                if current_allocation.cores == 0 && current_allocation.memory == 0 {
                    self.allocations.remove(&from);
                }
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}
