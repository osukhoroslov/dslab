use sugars::{rc, refcell};

use core::actor::{Actor, ActorContext, ActorId, Event};
use core::match_event;
use core::sim::Simulation;

// EVENTS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct Start {
    other: ActorId,
}

#[derive(Debug)]
pub struct Ping {}

#[derive(Debug)]
pub struct Pong {}

// ACTORS //////////////////////////////////////////////////////////////////////////////////////////

pub struct SimpleActor {}

impl SimpleActor {
    pub fn new() -> Self {
        Self {}
    }
}

impl Actor for SimpleActor {
    fn on(&mut self, event: Box<dyn Event>, from: ActorId, ctx: &mut ActorContext) {
        match_event!( event {
            Start { other } => {
                println!("[{}] received Start from {}", ctx.id, from);
                ctx.emit(Ping {}, other.clone(), 0.);
            },
            Ping {} => {
                println!("[{}] received Ping from {}", ctx.id, from);
                ctx.emit(Pong {}, from, 0.);
            },
            Pong {} => {
                println!("[{}] received Pong from {}", ctx.id, from);
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
    let app = ActorId::from("app");
    let actor1 = sim.add_actor("1", rc!(refcell!(SimpleActor::new())));
    let actor2 = sim.add_actor("2", rc!(refcell!(SimpleActor::new())));
    sim.add_event(Start { other: actor2.clone() }, app.clone(), actor1.clone(), 0.);
    sim.add_event(Start { other: actor1.clone() }, app.clone(), actor2.clone(), 0.);
    sim.step_until_no_events();
}
