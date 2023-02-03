use std::collections::HashMap;

use serde::Serialize;

use dslab_core::cast;
use dslab_core::component::Id;
use dslab_core::context::SimulationContext;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;

// STRUCTS /////////////////////////////////////////////////////////////////////////////////////////

#[derive(Serialize)]
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
#[derive(Serialize, Debug, Clone, Copy)]
pub enum CoresDependency {
    Linear,
    LinearWithFixed {
        fixed_part: f64,
    },
    Custom {
        #[serde(skip_serializing)]
        func: fn(u32) -> f64,
    },
}

impl CoresDependency {
    pub fn speedup(&self, cores: u32) -> f64 {
        match self {
            CoresDependency::Linear => cores as f64,
            CoresDependency::LinearWithFixed { fixed_part } => 1. / (fixed_part + (1. - fixed_part) / cores as f64),
            CoresDependency::Custom { func } => func(cores),
        }
    }
}

#[derive(Serialize, Debug)]
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
    requester: Id,
}

impl RunningComputation {
    fn new(cores: u32, memory: u64, requester: Id) -> Self {
        RunningComputation {
            cores,
            memory,
            requester,
        }
    }
}

// EVENTS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Serialize)]
pub struct CompRequest {
    pub flops: u64,
    pub memory: u64,
    pub min_cores: u32,
    pub max_cores: u32,
    pub cores_dependency: CoresDependency,
    pub requester: Id,
}

#[derive(Serialize)]
pub struct CompStarted {
    pub id: u64,
    pub cores: u32,
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

#[derive(Serialize)]
pub struct AllocationRequest {
    pub allocation: Allocation,
    pub requester: Id,
}

#[derive(Serialize)]
pub struct AllocationSuccess {
    pub id: u64,
}

#[derive(Serialize)]
pub struct AllocationFailed {
    pub id: u64,
    pub reason: FailReason,
}

#[derive(Serialize)]
pub struct DeallocationRequest {
    pub allocation: Allocation,
    pub requester: Id,
}

#[derive(Serialize)]
pub struct DeallocationSuccess {
    pub id: u64,
}

#[derive(Serialize)]
pub struct DeallocationFailed {
    pub id: u64,
    pub reason: FailReason,
}

// ACTORS //////////////////////////////////////////////////////////////////////////////////////////

pub struct Compute {
    speed: u64,
    cores_total: u32,
    cores_available: u32,
    memory_total: u64,
    memory_available: u64,
    computations: HashMap<u64, RunningComputation>,
    allocations: HashMap<Id, Allocation>,
    ctx: SimulationContext,
}

impl Compute {
    pub fn new(speed: u64, cores: u32, memory: u64, ctx: SimulationContext) -> Self {
        Self {
            speed,
            cores_total: cores,
            cores_available: cores,
            memory_total: memory,
            memory_available: memory,
            computations: HashMap::new(),
            allocations: HashMap::new(),
            ctx,
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
        &mut self,
        flops: u64,
        memory: u64,
        min_cores: u32,
        max_cores: u32,
        cores_dependency: CoresDependency,
        requester: Id,
    ) -> u64 {
        let request = CompRequest {
            flops,
            memory,
            min_cores,
            max_cores,
            cores_dependency,
            requester,
        };
        self.ctx.emit_self_now(request)
    }

    pub fn allocate(&mut self, cores: u32, memory: u64, requester: Id) -> u64 {
        let request = AllocationRequest {
            allocation: Allocation::new(cores, memory),
            requester,
        };
        self.ctx.emit_self_now(request)
    }

    pub fn deallocate(&mut self, cores: u32, memory: u64, requester: Id) -> u64 {
        let request = DeallocationRequest {
            allocation: Allocation::new(cores, memory),
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
                min_cores,
                max_cores,
                ref cores_dependency,
                requester,
            } => {
                if self.memory_available < memory || self.cores_available < min_cores {
                    self.ctx.emit_now(
                        CompFailed {
                            id: event.id,
                            reason: FailReason::NotEnoughResources {
                                available_cores: self.cores_available,
                                available_memory: self.memory_available,
                            },
                        },
                        requester,
                    );
                } else {
                    let cores = self.cores_available.min(max_cores);
                    self.memory_available -= memory;
                    self.cores_available -= cores;
                    self.ctx.emit_now(CompStarted { id: event.id, cores }, requester);

                    let speedup = cores_dependency.speedup(cores);

                    let compute_time = flops as f64 / self.speed as f64 / speedup;
                    self.ctx.emit_self(CompFinished { id: event.id }, compute_time);
                    self.computations
                        .insert(event.id, RunningComputation::new(cores, memory, requester));
                }
            }
            CompFinished { id } => {
                let running_computation = self
                    .computations
                    .remove(&id)
                    .expect("Unexpected CompFinished event in Compute");
                self.memory_available += running_computation.memory;
                self.cores_available += running_computation.cores;
                self.ctx.emit(CompFinished { id }, running_computation.requester, 0.);
            }
            AllocationRequest { allocation, requester } => {
                if self.memory_available < allocation.memory || self.cores_available < allocation.cores {
                    self.ctx.emit_now(
                        AllocationFailed {
                            id: event.id,
                            reason: FailReason::NotEnoughResources {
                                available_cores: self.cores_available,
                                available_memory: self.memory_available,
                            },
                        },
                        requester,
                    );
                } else {
                    let current_allocation = self
                        .allocations
                        .entry(requester)
                        .or_insert_with(|| Allocation::new(0, 0));
                    current_allocation.cores += allocation.cores;
                    current_allocation.memory += allocation.memory;
                    self.cores_available -= allocation.cores;
                    self.memory_available -= allocation.memory;
                    self.ctx.emit(AllocationSuccess { id: event.id }, requester, 0.);
                }
            }
            DeallocationRequest { allocation, requester } => {
                let current_allocation = self
                    .allocations
                    .entry(requester)
                    .or_insert_with(|| Allocation::new(0, 0));
                if current_allocation.cores >= allocation.cores && current_allocation.memory >= allocation.memory {
                    current_allocation.cores -= allocation.cores;
                    current_allocation.memory -= allocation.memory;
                    self.cores_available += allocation.cores;
                    self.memory_available += allocation.memory;
                    self.ctx.emit(DeallocationSuccess { id: event.id }, requester, 0.);
                } else {
                    self.ctx.emit_now(
                        DeallocationFailed {
                            id: event.id,
                            reason: FailReason::NotEnoughResources {
                                available_cores: current_allocation.cores,
                                available_memory: current_allocation.memory,
                            },
                        },
                        requester,
                    );
                }
                if current_allocation.cores == 0 && current_allocation.memory == 0 {
                    self.allocations.remove(&requester);
                }
            }
        })
    }
}
