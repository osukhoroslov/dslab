use core::match_event;
use core::actor::{Actor, ActorId, ActorContext, Event};

// EVENTS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct CompRequest {
    pub amount: u64,
}

#[derive(Debug)]
pub struct CompStarted {
}

#[derive(Debug)]
pub struct CompFinished {
}

#[derive(Debug)]
pub struct CompFailed {
}

// ACTORS //////////////////////////////////////////////////////////////////////////////////////////

pub struct ComputeActor {
    speed: u64,
}

impl ComputeActor {
    pub fn new(speed: u64) -> Self {
        Self {speed}
    }
}

impl Actor for ComputeActor {
    fn on(&mut self, event: Box<dyn Event>, from: &ActorId, ctx: &mut ActorContext) {
        match_event!( event {
            CompRequest { amount } => {
                println!("{} [{}] received CompRequest from {}", ctx.time(), ctx.id, from);
                let start_delay = 0.1;
                ctx.emit(CompStarted {}, from, start_delay);
                let compute_time = *amount as f64 / self.speed as f64;
                ctx.emit(CompFinished {}, from, start_delay + compute_time);
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}