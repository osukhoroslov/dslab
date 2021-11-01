use std::cell::RefCell;
use std::rc::Rc;

use core::sim::Simulation;
use core::actor::{Actor, ActorId, ActorContext};
use crate::Event::*;

// EVENTS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub enum Event {
    Start {
    },
    CompRequest {
        amount: u64,
    },
    CompStarted {
    },
    CompFinished {
    },
    CompFailed {
    }
}

// ACTORS //////////////////////////////////////////////////////////////////////////////////////////

pub struct TaskActor {
    amount: u64,
}

impl TaskActor {
    pub fn new(amount: u64) -> Self {
        Self {amount}
    }
}

impl Actor<Event> for TaskActor {
    fn on(&mut self, event: Event, from: ActorId, ctx: &mut ActorContext<Event>) {
        match event {
            Event::Start { } => {
                println!("{} [{}] received Start from {}", ctx.time(), ctx.id, from);
                let compute_actor = ActorId::from("compute");
                ctx.emit(CompRequest { amount: self.amount }, compute_actor, 0.);
            }
            Event::CompStarted {} => {
                println!("{} [{}] received CompStarted from {}", ctx.time(), ctx.id, from);
            },
            Event::CompFinished {} => {
                println!("{} [{}] received CompFinished from {}", ctx.time(), ctx.id, from);
            },
            Event::CompFailed {} => {
                println!("{} [{}] received CompFailed from {}", ctx.time(), ctx.id, from);
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
}

impl ComputeActor {
    pub fn new(speed: u64) -> Self {
        Self {speed}
    }
}

impl Actor<Event> for ComputeActor {
    fn on(&mut self, event: Event, from: ActorId, ctx: &mut ActorContext<Event>) {
        match event {
            Event::CompRequest { amount } => {
                println!("{} [{}] received CompRequest from {}", ctx.time(), ctx.id, from);
                let start_delay = 0.1;
                ctx.emit(CompStarted {}, from.clone(), start_delay);
                let compute_time = amount as f64 / self.speed as f64;
                ctx.emit(CompFinished {}, from, start_delay + compute_time);
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
    sim.add_actor("task", Rc::new(RefCell::new(TaskActor::new(100))));
    sim.add_actor("compute", Rc::new(RefCell::new(ComputeActor::new(10))));
    sim.add_event(Start {}, "0", "task", 0.);
    sim.step_until_no_events();
}