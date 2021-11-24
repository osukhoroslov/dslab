use sugars::{rc, refcell};

use core::actor::{Actor, ActorContext, ActorId, Event};
use core::match_event;
use core::sim::Simulation;

// EVENTS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct Start {}

#[derive(Debug)]
pub struct CompRequest {
    amount: u64,
}

#[derive(Debug)]
pub struct CompStarted {}

#[derive(Debug)]
pub struct CompFinished {}

#[derive(Debug)]
pub struct CompFailed {}

// ACTORS //////////////////////////////////////////////////////////////////////////////////////////

pub struct TaskActor {
    amount: u64,
    compute: ActorId,
}

impl TaskActor {
    pub fn new(amount: u64, compute: ActorId) -> Self {
        Self { amount, compute }
    }
}

impl Actor for TaskActor {
    fn on(&mut self, event: Box<dyn Event>, from: ActorId, ctx: &mut ActorContext) {
        match_event!( event {
            Start {} => {
                println!("{} [{}] received Start from {}", ctx.time(), ctx.id, from);
                ctx.emit(CompRequest { amount: self.amount }, self.compute.clone(), 0.);
            },
            CompStarted {} => {
                println!("{} [{}] received CompStarted from {}", ctx.time(), ctx.id, from);
            },
            CompFinished {} => {
                println!("{} [{}] received CompFinished from {}", ctx.time(), ctx.id, from);
            },
            CompFailed {} => {
                println!("{} [{}] received CompFailed from {}", ctx.time(), ctx.id, from);
            },
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}

pub struct ComputeActor {
    speed: u64,
}

impl ComputeActor {
    pub fn new(speed: u64) -> Self {
        Self { speed }
    }
}

impl Actor for ComputeActor {
    fn on(&mut self, event: Box<dyn Event>, from: ActorId, ctx: &mut ActorContext) {
        match_event!( event {
            CompRequest { amount } => {
                println!("{} [{}] received CompRequest from {}", ctx.time(), ctx.id, from);
                let start_delay = 0.1;
                ctx.emit(CompStarted {}, from.clone(), start_delay);
                let compute_time = *amount as f64 / self.speed as f64;
                ctx.emit(CompFinished {}, from, start_delay + compute_time);
            },
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
    let task = sim.add_actor("task", rc!(refcell!(TaskActor::new(100, compute))));
    sim.add_event(Start {}, ActorId::from("app"), task, 0.);
    sim.step_until_no_events();
}
