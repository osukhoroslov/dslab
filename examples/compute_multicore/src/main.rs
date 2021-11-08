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

#[derive(Debug, Clone)]
pub enum Event {
    Start {
    },
    CompRequest {
        computation: Computation,
        cores: u64,
    },
    CompStarted {
        computation: Computation,
    },
    CompFinished {
        computation: Computation,
    },
    CompFailed {
        computation: Computation,
    },
    AllocationRequest {
        allocation: Allocation,
    },
    AllocationSuccess {
        allocation: Allocation,
    },
    AllocationFailed {
        allocation: Allocation,
    },
    DeallocationRequest {
        allocation: Allocation,
    },
    DeallocationSuccess {
        allocation: Allocation,
    },
    DeallocationFailed {
        allocation: Allocation,
    },
}

// ACTORS //////////////////////////////////////////////////////////////////////////////////////////

pub struct TaskActor {
    task: Computation,
    cores: u64,
}

impl TaskActor {
    pub fn new(computation: Computation, cores: u64) -> Self {
        Self {
            task: computation,
            cores: cores,
        }
    }
}

impl Actor<Event> for TaskActor {
    fn on(&mut self, event: Event, from: ActorId, _event_id: u64, ctx: &mut ActorContext<Event>) {
        match event {
            Event::Start {} => {
                println!("{} [{}] received Start from {}", ctx.time(), ctx.id, from);
                let compute_actor = ActorId::from("compute");
                ctx.emit(CompRequest { computation: self.task.clone(), cores: self.cores }, compute_actor, 0.);
            }
            Event::CompStarted { computation } => {
                println!("{} [{}] received CompStarted from {} for {:?}", ctx.time(), ctx.id, from, computation);
            },
            Event::CompFinished { computation } => {
                println!("{} [{}] received CompFinished from {} for {:?}", ctx.time(), ctx.id, from, computation);
            },
            Event::CompFailed { computation } => {
                println!("{} [{}] received CompFailed from {} for {:?}", ctx.time(), ctx.id, from, computation);
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
            Event::AllocationFailed { allocation } => {
                println!("{} [{}] received AllocationFailed from {} for {:?}", ctx.time(), ctx.id, from, allocation);
            },
            Event::DeallocationFailed { allocation } => {
                println!("{} [{}] received DeallocationFailed from {} for {:?}", ctx.time(), ctx.id, from, allocation);
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
        match event {
            Event::CompRequest { ref computation, cores } => {
                if self.memory < computation.memory || self.cores < cores {
                    ctx.emit(CompFailed { computation: computation.clone() }, from, 0.);
                } else {
                    println!("{} [{}] received {:?} from {}", ctx.time(), ctx.id, event, from);
                    self.memory -= computation.memory;
                    self.cores -= cores;
                    ctx.emit(CompStarted { computation: computation.clone() }, from.clone(), 0.);
                    let compute_time = computation.flops as f64 / self.speed as f64 / cores as f64;
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
                    ctx.emit(AllocationFailed { allocation }, from, 0.);
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
                    ctx.emit(DeallocationFailed { allocation }, from.clone(), 0.);
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
    {
        let mut sim = Simulation::<Event>::new(123);
        sim.add_actor("task1", Rc::new(RefCell::new(TaskActor::new(Computation::new(100, 512, 1), 2))));
        sim.add_actor("task2", Rc::new(RefCell::new(TaskActor::new(Computation::new(200, 512, 2), 4))));
        sim.add_actor("compute", Rc::new(RefCell::new(ComputeActor::new(10, 6, 1024))));
        sim.add_event(Start {}, "0", "task1", 0.);
        sim.add_event(Start {}, "0", "task2", 0.);
        sim.step_until_no_events();
    }
    println!("==================================================================================");
    {
        let mut sim = Simulation::<Event>::new(123);
        sim.add_actor("task1", Rc::new(RefCell::new(AllocationActor::new(Allocation::new(2, 512), 3.0))));
        sim.add_actor("task2", Rc::new(RefCell::new(AllocationActor::new(Allocation::new(5, 512), 4.0))));
        sim.add_actor("compute", Rc::new(RefCell::new(ComputeActor::new(10, 6, 1024))));
        sim.add_event(Start {}, "0", "task1", 0.);
        sim.add_event(Start {}, "0", "task2", 3.1);
        sim.step_until_no_events();
    }
}
