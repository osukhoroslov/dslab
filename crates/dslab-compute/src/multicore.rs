//! Model of computing resource with multiple cores.

use std::collections::HashMap;

use serde::Serialize;

use dslab_core::cast;
use dslab_core::component::Id;
use dslab_core::context::SimulationContext;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;

// STRUCTS -------------------------------------------------------------------------------------------------------------

/// Resource allocation.
#[derive(Clone, Serialize)]
pub struct Allocation {
    /// Number of cores.
    pub cores: u32,
    /// Amount of memory.
    pub memory: u64,
}

impl Allocation {
    /// Creates a new allocation.
    pub fn new(cores: u32, memory: u64) -> Self {
        Self { cores, memory }
    }
}

/// Function from `[1, max_cores]` to `[1, +inf]` describing the dependency
/// between the number of cores used for running a task and achieved parallel speedup.
#[derive(Clone, Copy, Debug, Serialize)]
pub enum CoresDependency {
    /// Linear dependency: `speedup(cores) = cores`
    Linear,
    /// Linear dependency with fixed part corresponding to Amdahl's law:
    /// `speedup(cores) = 1 / (fixed_part + (1 - fixed_part) / cores)`
    LinearWithFixed {
        /// Fraction of a computation which can't be parallelized.
        fixed_part: f64,
    },
    /// Custom dependency.
    Custom {
        #[serde(skip_serializing)]
        /// Custom speedup function.
        func: fn(u32) -> f64,
    },
}

impl CoresDependency {
    /// Speedup achieved when using the given number of cores compared to using a single core.
    pub fn speedup(&self, cores: u32) -> f64 {
        match self {
            CoresDependency::Linear => cores as f64,
            CoresDependency::LinearWithFixed { fixed_part } => 1. / (fixed_part + (1. - fixed_part) / cores as f64),
            CoresDependency::Custom { func } => func(cores),
        }
    }
}

/// Reason for computation failure.
#[derive(Clone, Debug, Serialize)]
pub enum FailReason {
    /// Resource doesn't have enough memory or available cores.
    NotEnoughResources {
        /// Currently available number of cores.
        available_cores: u32,
        /// Currently available amount of memory.
        available_memory: u64,
        /// Requested number of cores.
        requested_cores: u32,
        /// Requested amount of memory.
        requested_memory: u64,
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

// EVENTS --------------------------------------------------------------------------------------------------------------

/// Request to start a computation.
#[derive(Clone, Serialize)]
pub struct CompRequest {
    /// Total computation size.
    pub flops: f64,
    /// Total memory needed for a computation.
    pub memory: u64,
    /// Minimum number of used cores.
    pub min_cores: u32,
    /// Maximum number of used cores.
    pub max_cores: u32,
    /// Defines the dependence of parallel speedup on the number of used cores.
    pub cores_dependency: CoresDependency,
    /// Id of simulation component to inform about the computation progress.
    pub requester: Id,
}

/// Computation is started successfully.
#[derive(Clone, Serialize)]
pub struct CompStarted {
    /// Id of the computation.
    pub id: u64,
    /// Number of cores allocated to the computation.
    /// Equals to the minimum between the number of available cores
    /// and the maximum number of cores for the computation.
    pub cores: u32,
}

/// Computation is finished successfully.
#[derive(Clone, Serialize)]
pub struct CompFinished {
    /// Id of the computation.
    pub id: u64,
}

/// Computation is failed.
#[derive(Clone, Serialize)]
pub struct CompFailed {
    /// Id of the computation.
    pub id: u64,
    /// Reason for failure.
    pub reason: FailReason,
}

/// Request to allocate resources.
#[derive(Clone, Serialize)]
pub struct AllocationRequest {
    /// Allocated resource.
    pub allocation: Allocation,
    /// Id of simulation component to inform about the allocation result.
    pub requester: Id,
}

/// Allocation is successful.
#[derive(Clone, Serialize)]
pub struct AllocationSuccess {
    /// Id of the allocation.
    pub id: u64,
}

/// Allocation is failed.
#[derive(Clone, Serialize)]
pub struct AllocationFailed {
    /// Id of the allocation.
    pub id: u64,
    /// Reason for failure.
    pub reason: FailReason,
}

/// Request to release previously allocated resources.
#[derive(Clone, Serialize)]
pub struct DeallocationRequest {
    /// Released resources.
    pub allocation: Allocation,
    /// Id of simulation component to inform about the deallocation result.
    pub requester: Id,
}

/// Deallocation is successful.
#[derive(Clone, Serialize)]
pub struct DeallocationSuccess {
    /// Id of the deallocation.
    pub id: u64,
}

/// Deallocation is failed.
#[derive(Clone, Serialize)]
pub struct DeallocationFailed {
    /// Id of the deallocation.
    pub id: u64,
    /// Reason for failure.
    pub reason: FailReason,
}

// MODEL ---------------------------------------------------------------------------------------------------------------

/// Models computing resource with multiple cores which supports execution of parallel tasks.
///
/// In this model, the computation request can specify the minimum and maximum number of used cores,
/// and provide a function which defines the dependence of parallel speedup on the number of used cores.
/// Each core can only be used by one computation. The cores allocation for each computation is computed
/// upon the request arrival and is not changed afterwards.
/// This model also supports the manual allocation and release of cores and memory.
pub struct Compute {
    speed: f64,
    cores_total: u32,
    cores_available: u32,
    memory_total: u64,
    memory_available: u64,
    computations: HashMap<u64, RunningComputation>,
    allocations: HashMap<Id, Allocation>,
    ctx: SimulationContext,
}

impl Compute {
    /// Creates a new computing resource.
    pub fn new(speed: f64, cores: u32, memory: u64, ctx: SimulationContext) -> Self {
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
    pub fn id(&self) -> Id {
        self.ctx.id()
    }

    /// Returns the core speed.
    pub fn speed(&self) -> f64 {
        self.speed
    }

    /// Returns the total number of cores.
    pub fn cores_total(&self) -> u32 {
        self.cores_total
    }

    /// Returns the number of available cores.
    pub fn cores_available(&self) -> u32 {
        self.cores_available
    }

    /// Returns the total amount of memory.
    pub fn memory_total(&self) -> u64 {
        self.memory_total
    }

    /// Returns the amount of available memory.
    pub fn memory_available(&self) -> u64 {
        self.memory_available
    }

    /// Starts computation with given parameters and returns computation id.
    pub fn run(
        &mut self,
        flops: f64,
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

    /// Requests resource allocation with given parameters and returns allocation id.
    pub fn allocate(&mut self, cores: u32, memory: u64, requester: Id) -> u64 {
        let request = AllocationRequest {
            allocation: Allocation::new(cores, memory),
            requester,
        };
        self.ctx.emit_self_now(request)
    }

    /// Requests resource deallocation with given parameters and returns deallocation id.
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
                                requested_cores: min_cores,
                                requested_memory: memory,
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

                    let compute_time = flops / self.speed / speedup;
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
                                requested_cores: allocation.cores,
                                requested_memory: allocation.memory,
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
                                requested_cores: allocation.cores,
                                requested_memory: allocation.memory,
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
