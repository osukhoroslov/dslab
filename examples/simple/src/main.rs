use std::cell::RefCell;
use std::rc::Rc;

use core::sim::Simulation;
use core::actor::{Actor, ActorId, ActorContext};
use crate::Event::*;

// EVENTS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub enum Event {
    Start {
        other: String,
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
                ctx.emit(Ping {}, ActorId::from(&other), 0.);
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
    let actor1 = Rc::new(RefCell::new(SimpleActor::new()));
    let actor2 = Rc::new(RefCell::new(SimpleActor::new()));
    sim.add_actor("1", actor1);
    sim.add_actor("2", actor2);
    sim.add_event(Start {other: "2".to_string()}, "0", "1", 0.);
    sim.add_event(Start {other: "1".to_string()}, "0", "2", 0.);
    sim.step_until_no_events();
}