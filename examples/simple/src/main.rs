use std::cell::RefCell;
use std::rc::Rc;

use core::sim::Simulation;
use core::actor::{Actor, ActorId, ActorContext};
use crate::Event::*;

// EVENTS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub enum Event {
    Start {
        other: ActorId,
    },
    Ping {
    },
    Pong {
    }
}

// ACTORS //////////////////////////////////////////////////////////////////////////////////////////

pub struct SimpleActor {
}

impl SimpleActor {
    pub fn new() -> Self {
        Self {}
    }
}

impl Actor<Event> for SimpleActor {
    fn on(&mut self, event: Event, from: ActorId, ctx: &mut ActorContext<Event>) {
        match event {
            Event::Start { other } => {
                println!("[{}] received Start from {}", ctx.id, from);
                ctx.emit(Ping {}, other, 0.);
            }
            Event::Ping {} => {
                println!("[{}] received Ping from {}", ctx.id, from);
                ctx.emit(Pong {}, from, 0.);
            },
            Event::Pong {} => {
                println!("[{}] received Pong from {}", ctx.id, from);
            }
        }
    }

    fn is_active(&self) -> bool {
        true
    }
}

// MAIN ////////////////////////////////////////////////////////////////////////////////////////////

fn main() {
    let mut sim = Simulation::<Event>::new(123);
    let actor1_id = ActorId::from("1");
    let actor2_id = ActorId::from("2");
    let actor1 = Rc::new(RefCell::new(SimpleActor::new()));
    let actor2 = Rc::new(RefCell::new(SimpleActor::new()));
    sim.add_actor(actor1_id.clone(), actor1);
    sim.add_actor(actor2_id.clone(), actor2);
    sim.add_event(Start {other: actor2_id.clone()}, ActorId::from("0"), actor1_id.clone(), 0.);
    sim.add_event(Start {other: actor1_id.clone()}, ActorId::from("0"), actor2_id.clone(), 0.);
    sim.step_until_no_events();
}