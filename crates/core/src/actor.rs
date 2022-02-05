use downcast_rs::{impl_downcast, Downcast};
use rand::prelude::*;
use rand_pcg::Pcg64;
use std::fmt::{Debug, Error, Formatter};
use std::hash::{Hash, Hasher};

// EVENT ///////////////////////////////////////////////////////////////////////////////////////////

pub trait Event: Downcast + Debug {}

impl_downcast!(Event);

impl<T: Debug + 'static> Event for T {}

// ACTOR ///////////////////////////////////////////////////////////////////////////////////////////

pub trait Actor {
    fn on(&mut self, event: Box<dyn Event>, from: ActorId, ctx: &mut ActorContext);
    fn is_active(&self) -> bool;
}

// ACTOR ID ////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialOrd, Ord)]
pub struct ActorId(pub String);

impl ActorId {
    pub fn from(str: &str) -> Self {
        ActorId(str.to_string())
    }
    pub fn to(&self) -> String {
        self.0.clone()
    }
}

impl PartialEq for ActorId {
    fn eq(&self, other: &ActorId) -> bool {
        self.0 == other.0
    }
}

impl Eq for ActorId {}

impl Hash for ActorId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
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

pub struct CtxEvent {
    pub(crate) event: Box<dyn Event>,
    pub(crate) dest: ActorId,
    pub(crate) delay: f64,
}

pub struct ActorContext<'a> {
    pub id: ActorId,
    pub event_id: u64,
    pub(crate) time: f64,
    pub(crate) rand: &'a mut Pcg64,
    pub(crate) next_event_id: u64,
    pub(crate) events: Vec<CtxEvent>,
    pub(crate) canceled_events: Vec<u64>,
}

impl<'a> ActorContext<'a> {
    pub fn time(&self) -> f64 {
        self.time
    }

    pub fn emit<T: Event>(&mut self, event: T, dest: ActorId, delay: f64) -> u64 {
        self.emit_any(Box::new(event), dest, delay)
    }

    pub fn emit_now<T: Event>(&mut self, event: T, dest: ActorId) -> u64 {
        self.emit(event, dest, 0.)
    }

    pub fn emit_self<T: Event>(&mut self, event: T, delay: f64) -> u64 {
        self.emit(event, self.id.clone(), delay)
    }

    pub fn emit_any(&mut self, event: Box<dyn Event>, dest: ActorId, delay: f64) -> u64 {
        let entry = CtxEvent { event, dest, delay };
        self.events.push(entry);
        self.next_event_id += 1;
        self.next_event_id - 1
    }

    pub fn rand(&mut self) -> f64 {
        self.rand.gen_range(0.0..1.0)
    }

    pub fn cancel_event(&mut self, event_id: u64) {
        // println!("Canceled event: {}", event_id);
        self.canceled_events.push(event_id);
    }
}

// MACRO ///////////////////////////////////////////////////////////////////////////////////////////

#[macro_export]
macro_rules! match_event {
    ( $event:ident { $( $pattern:pat => $arm:block ),+ $(,)? } ) => {
        $(
            if let Some($pattern) = $event.downcast_ref() {
                $arm
            } else
        )*
        {
            println!("Unknown event: {:?}", $event)
        }
    }
}

#[macro_export]
macro_rules! cast {
    ( match $event:ident { $( $pattern:pat => $arm:block )+ } ) => {
        $(
            if let Some($pattern) = $event.downcast_ref() {
                $arm
            } else
        )*
        {
            println!("Unknown event: {:?}", $event)
        }
    }
}
