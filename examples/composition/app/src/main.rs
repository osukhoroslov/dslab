use sugars::{refcell, rc};

use core::match_event;
use core::sim::Simulation;
use core::actor::{Actor, ActorId, ActorContext, Event};
use compute::*;

// EVENTS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct Start {
}

#[derive(Debug)]
pub struct TaskAssigned {
    amount: u64
}

#[derive(Debug)]
pub struct TaskCompleted {
}

// ACTORS //////////////////////////////////////////////////////////////////////////////////////////

pub struct Master {
    worker: ActorId,
}

impl Master {
    pub fn new(worker: ActorId) -> Self {
        Self { worker }
    }
}

impl Actor for Master {
    fn on(&mut self, event: Box<dyn Event>, from: ActorId, ctx: &mut ActorContext) {
        match_event!( event {
            Start {} => {
                println!("{} [{}] received Start from {}", ctx.time(), ctx.id, from);
                ctx.emit(TaskAssigned { amount: 100 }, self.worker.clone(), 0.);
            },
            TaskCompleted {} => {
                println!("{} [{}] received TaskCompleted from {}", ctx.time(), ctx.id, from);
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}

pub struct Worker {
    compute: ActorId,
    master: Option<ActorId>,
}

impl Worker {
    pub fn new(compute: ActorId) -> Self {
        Self { compute, master: None }
    }
}

impl Actor for Worker {
    fn on(&mut self, event: Box<dyn Event>, from: ActorId, ctx: &mut ActorContext) {
        match_event!( event {
            TaskAssigned { amount } => {
                println!("{} [{}] received TaskAssigned from {}", ctx.time(), ctx.id, from);
                ctx.emit(CompRequest { amount: *amount }, self.compute.clone(), 0.);
                self.master = Some(from.clone());
            },
            CompStarted {} => {
                println!("{} [{}] received CompStarted from {}", ctx.time(), ctx.id, from);
            },
            CompFinished {} => {
                println!("{} [{}] received CompFinished from {}", ctx.time(), ctx.id, from);
                ctx.emit(TaskCompleted {}, self.master.clone().unwrap(), 0.);
            },
            CompFailed {} => {
                println!("{} [{}] received CompFailed from {}", ctx.time(), ctx.id, from);
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}

// MAIN ////////////////////////////////////////////////////////////////////////////////////////////

fn main() {
    let mut sim = Simulation::new(123);
    let compute = sim.add_actor("compute", rc!(refcell!(ComputeActor::new(10))));
    let worker = sim.add_actor("worker", rc!(refcell!(Worker::new(compute))));
    let master = sim.add_actor("master", rc!(refcell!(Master::new(worker))));
    sim.add_event(Start { }, ActorId::from("app"), master, 0.);
    sim.step_until_no_events();
}