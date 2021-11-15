use std::cell::RefCell;
use std::rc::Rc;
use std::collections::HashMap;

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
pub struct Allocation {
    cores: u64,
    memory: u64,
}

impl Allocation {
    pub fn new(cores: u64, memory: u64) -> Self {
        Self {
            cores: cores,
            memory: memory,
        }
    }
}

// [1 .. max_cores] -> [0, 1]
#[derive(Debug, Clone)]
pub enum CoresDependency {
    Linear,
    LinearWithFixed {
        fixed_part: f64,
    },
    Custom {
        func: fn(u64) -> f64,
    },
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

#[derive(Debug, Clone)]
pub enum Event {
    Start {
    },
    CompRequest {
        computation: Computation,
        min_cores: u64,
        max_cores: u64,
        cores_dependency: CoresDependency,
    },
    CompStarted {
        computation: Computation,
        cores: u64,
    },
    CompFinished {
        computation: Computation,
    },
    CompFailed {
        computation: Computation,
        reason: FailReason,
    },
    AllocationRequest {
        allocation: Allocation,
    },
    AllocationSuccess {
        allocation: Allocation,
    },
    AllocationFailed {
        allocation: Allocation,
        reason: FailReason,
    },
    DeallocationRequest {
        allocation: Allocation,
    },
    DeallocationSuccess {
        allocation: Allocation,
    },
    DeallocationFailed {
        allocation: Allocation,
        reason: FailReason,
    },
}

// ACTORS //////////////////////////////////////////////////////////////////////////////////////////

pub struct TaskActor {
    task: Computation,
    min_cores: u64,
    max_cores: u64,
    cores_dependency: CoresDependency,
}

impl TaskActor {
    pub fn new(computation: Computation, min_cores: u64, max_cores: u64, cores_dependency: CoresDependency) -> Self {
        Self {
            task: computation,
            min_cores: min_cores,
            max_cores: max_cores,
            cores_dependency: cores_dependency,
        }
    }
}

impl Actor<Event> for TaskActor {
    fn on(&mut self, event: Event, from: ActorId, _event_id: u64, ctx: &mut ActorContext<Event>) {
        match event {
            Event::Start {} => {
                println!("{} [{}] received Start from {}", ctx.time(), ctx.id, from);
                let compute_actor = ActorId::from("compute");
                ctx.emit(CompRequest {
                                       computation: self.task.clone(),
                                       min_cores: self.min_cores,
                                       max_cores: self.max_cores,
                                       cores_dependency: self.cores_dependency.clone(),
                                     },
                         compute_actor, 0.);
            }
            Event::CompStarted { computation, cores } => {
                println!("{} [{}] received CompStarted from {} for {:?} on {} cores", ctx.time(), ctx.id, from, computation, cores);
            },
            Event::CompFinished { computation } => {
                println!("{} [{}] received CompFinished from {} for {:?}", ctx.time(), ctx.id, from, computation);
            },
            Event::CompFailed { computation, reason } => {
                println!("{} [{}] received CompFailed from {} for {:?} with reason {:?}", ctx.time(), ctx.id, from, computation, reason);
            },
            _ => ()
        }
    }

    fn is_active(&self) -> bool {
        true
    }
}




pub struct AllocationActor {
    allocation: Allocation,
    time: f64,
}

impl AllocationActor {
    pub fn new(allocation: Allocation, time: f64) -> Self {
        Self {
            allocation: allocation,
            time: time,
        }
    }
}

impl Actor<Event> for AllocationActor {
    fn on(&mut self, event: Event, from: ActorId, _event_id: u64, ctx: &mut ActorContext<Event>) {
        match event {
            Event::Start {} => {
                println!("{} [{}] received Start from {}", ctx.time(), ctx.id, from);
                let compute_actor = ActorId::from("compute");
                ctx.emit(AllocationRequest { allocation: self.allocation.clone() }, compute_actor.clone(), 0.);
                ctx.emit(DeallocationRequest { allocation: self.allocation.clone() }, compute_actor, self.time);
            }
            Event::AllocationFailed { allocation, reason } => {
                println!("{} [{}] received AllocationFailed from {} for {:?} with reason {:?}", ctx.time(), ctx.id, from, allocation, reason);
            },
            Event::DeallocationFailed { allocation, reason } => {
                println!("{} [{}] received DeallocationFailed from {} for {:?} with reason {:?}", ctx.time(), ctx.id, from, allocation, reason);
            },
            Event::AllocationSuccess { allocation } => {
                println!("{} [{}] received AllocationSuccess from {} for {:?}", ctx.time(), ctx.id, from, allocation);
            },
            Event::DeallocationSuccess { allocation } => {
                println!("{} [{}] received DeallocationSuccess from {} for {:?}", ctx.time(), ctx.id, from, allocation);
            },
            _ => ()
        }
    }

    fn is_active(&self) -> bool {
        true
    }
}



pub struct ComputeActor {
    speed: u64,
    cores: u64,
    memory: u64,
    current_computations: HashMap<u64, (u64, ActorId)>,
    current_allocations: HashMap<ActorId, Allocation>,
}

impl ComputeActor {
    pub fn new(speed: u64, cores: u64, memory: u64) -> Self {
        Self {
            speed: speed,
            cores: cores,
            memory: memory,
            current_computations: HashMap::new(),
            current_allocations: HashMap::new(),
        }
    }
}

impl Actor<Event> for ComputeActor {
    fn on(&mut self, event: Event, from: ActorId, event_id: u64, ctx: &mut ActorContext<Event>) {
        println!("{} [{}] received {:?} from {}", ctx.time(), ctx.id, event, from);
        match event {
            Event::CompRequest { ref computation, min_cores, max_cores, ref cores_dependency } => {
                if self.memory < computation.memory || self.cores < min_cores {
                    ctx.emit(CompFailed { computation: computation.clone(), reason: FailReason::NotEnoughResources { available_cores: self.cores, available_memory: self.memory } }, from, 0.);
                } else {
                    let cores = self.cores.min(max_cores);
                    self.memory -= computation.memory;
                    self.cores -= cores;
                    ctx.emit(CompStarted { computation: computation.clone(), cores }, from.clone(), 0.);

                    let multithreading_coefficient = match cores_dependency {
                        CoresDependency::Linear => {
                            1. / cores as f64
                        },
                        CoresDependency::LinearWithFixed { fixed_part } => {
                            fixed_part + (1. - fixed_part) / cores as f64
                        },
                        CoresDependency::Custom { func } => {
                            func(cores)
                        },
                    };

                    let compute_time = computation.flops as f64 / self.speed as f64 * multithreading_coefficient;
                    let finish_event_id = ctx.emit(CompFinished { computation: computation.clone() }, ctx.id.clone(), compute_time);
                    self.current_computations.insert(finish_event_id, (cores, from.clone()));
                }
            },
            Event::CompFinished { computation } => {
                let (cores, actor_id) = self.current_computations.remove(&event_id).expect("Unexpected CompFinished event in ComputeActor");
                self.memory += computation.memory;
                self.cores += cores;
                ctx.emit(CompFinished { computation: computation.clone() }, actor_id, 0.);
            },
            Event::AllocationRequest { allocation } => {
                if self.memory < allocation.memory || self.cores < allocation.cores {
                    ctx.emit(AllocationFailed { allocation: allocation, reason: FailReason::NotEnoughResources { available_cores: self.cores, available_memory: self.memory } }, from, 0.);
                } else {
                    let current_allocation = self.current_allocations.entry(from.clone()).or_insert(Allocation::new(0, 0));
                    current_allocation.cores += allocation.cores;
                    current_allocation.memory += allocation.memory;
                    self.cores -= allocation.cores;
                    self.memory -= allocation.memory;
                    ctx.emit(AllocationSuccess { allocation }, from, 0.);
                }
            },
            Event::DeallocationRequest { allocation } => {
                let current_allocation = self.current_allocations.entry(from.clone()).or_insert(Allocation::new(0, 0));
                if current_allocation.cores >= allocation.cores && current_allocation.memory >= allocation.memory {
                    current_allocation.cores -= allocation.cores;
                    current_allocation.memory -= allocation.memory;
                    self.cores += allocation.cores;
                    self.memory += allocation.memory;
                    ctx.emit(DeallocationSuccess { allocation }, from.clone(), 0.);
                } else {
                    ctx.emit(DeallocationFailed { allocation: allocation, reason: FailReason::NotEnoughResources { available_cores: self.cores, available_memory: self.memory } }, from.clone(), 0.);
                }
                if current_allocation.cores == 0 && current_allocation.memory == 0 {
                    self.current_allocations.remove(&from);
                }
            },
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
    sim.add_actor("task1", Rc::new(RefCell::new(TaskActor::new(
        Computation::new(100, 512, 1),
        2,
        6,
        CoresDependency::Linear,
    ))));
    sim.add_actor("task2", Rc::new(RefCell::new(TaskActor::new(
        Computation::new(100, 512, 2),
        4,
        10,
        CoresDependency::LinearWithFixed{ fixed_part: 0.4 },
    ))));
    sim.add_actor("task3", Rc::new(RefCell::new(TaskActor::new(
        Computation::new(100, 512, 3),
        5,
        7,
        CoresDependency::Custom{ func: |cores: u64| -> f64 {
            if cores == 7 {
                0.5
            } else {
                1.0
            }
        } },
    ))));
    sim.add_actor("task4", Rc::new(RefCell::new(TaskActor::new(
        Computation::new(100, 512, 4),
        15,
        20,
        CoresDependency::Linear,
    ))));
    sim.add_actor("compute", Rc::new(RefCell::new(ComputeActor::new(1, 10, 1024))));
    sim.add_event(Start {}, "0", "task1", 0.);
    sim.add_event(Start {}, "0", "task2", 0.);
    sim.add_event(Start {}, "0", "task3", 1000.);
    sim.add_event(Start {}, "0", "task4", 2000.);
    sim.step_until_no_events();
}
