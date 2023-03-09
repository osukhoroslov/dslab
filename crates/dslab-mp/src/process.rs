use std::fmt::Debug;
use std::hash::{Hash, Hasher};

use downcast_rs::{impl_downcast, Downcast};
use dyn_clone::{clone_trait_object, DynClone};

use crate::context::Context;
use crate::message::Message;

pub trait ProcessState: Downcast + Debug {
    fn derived_hash(&self, hasher: &mut Box<&mut dyn Hasher>);
    fn derived_eq(&self, other: &Box<dyn ProcessState>) -> bool;
}

impl_downcast!(ProcessState);

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct ProcessStateStub {}

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct StringProcessState {
    str: String,
}

impl StringProcessState {
    pub fn new(str: String) -> Self {
        Self { str }
    }

    pub fn str(&self) -> &String {
        return &self.str;
    }
}

impl<T: Hash + Eq + Debug + 'static> ProcessState for T {
    fn derived_hash(&self, hasher: &mut Box<&mut dyn Hasher>) {
        self.hash(&mut (*hasher));
    }
    fn derived_eq(&self, other: &Box<dyn ProcessState>) -> bool {
        if let Some(other) = other.downcast_ref::<T>() {
            self.eq(&(*other))
        } else {
            false
        }
    }
}


pub trait Process: DynClone {
    /// Called when a message is received.
    fn on_message(&mut self, msg: Message, from: String, ctx: &mut Context);

    /// Called when a _local_ message is received.
    fn on_local_message(&mut self, msg: Message, ctx: &mut Context);

    /// Called when a timer fires.
    fn on_timer(&mut self, timer: String, ctx: &mut Context);

    /// Returns the maximum size of process inner data observed so far.
    fn max_size(&mut self) -> u64 {
        0
    }

    /// Returns the process state.
    fn state(&self) -> Box<dyn ProcessState> {
        Box::new(ProcessStateStub {})
    }

    /// Restores the process state.
    fn set_state(&self, _state: Box<dyn ProcessState>) {}
}

clone_trait_object!(Process);
