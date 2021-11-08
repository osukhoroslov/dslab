use std::fmt::{Debug, Error, Formatter};
use rand::prelude::*;
use rand_pcg::Pcg64;


// ACTOR ///////////////////////////////////////////////////////////////////////////////////////////

pub trait Actor<E: Debug> {
    fn on(&mut self, event: E, from: ActorId, event_id: u64, ctx: &mut ActorContext<E>);
    fn is_active(&self) -> bool;
}

// ACTOR ID ////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Eq, PartialEq, Hash)]
pub struct ActorId(pub String);

impl ActorId {
    pub fn from(str: &str) -> Self {
        ActorId(str.to_string())
    }
    pub fn to(&self) -> String {
        self.0.clone()
    }
}

impl std::fmt::Display for ActorId {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "{}", self.0)
    }
}

impl std::fmt::Debug for ActorId {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "{}", self.0)
    }
}

// ACTOR CONTEXT ///////////////////////////////////////////////////////////////////////////////////

pub struct CtxEvent<E> {
    pub(crate) event: E,
    pub(crate) dest: ActorId,
    pub(crate) delay: f64
}

pub struct ActorContext<'a, E: Debug> {
    pub id: ActorId,
    pub(crate) time: f64,
    pub(crate) rand: &'a mut Pcg64,
    pub(crate) next_event_id: u64,
    pub(crate) events: Vec<CtxEvent<E>>,
    pub(crate) canceled_events: Vec<u64>,
}

impl<'a, E: Debug> ActorContext<'a, E> {
    pub fn time(&self) -> f64 {
        self.time
    }

    pub fn emit(&mut self, event: E, dest: ActorId, delay: f64) -> u64 {
        let entry = CtxEvent{ event, dest, delay };
        self.events.push(entry);
        self.next_event_id += 1;
        self.next_event_id - 1
    }

    pub fn rand(&mut self) -> f64 {
        self.rand.gen_range(0.0 .. 1.0)
    }

    pub fn cancel_event(&mut self, event_id: u64) {
        // println!("Canceled event: {}", event_id);
        self.canceled_events.push(event_id);
    }
}
