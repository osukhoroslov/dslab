use std::fmt::Debug;
use std::hash::{Hash, Hasher};

use downcast_rs::{impl_downcast, Downcast};
use dyn_clone::{clone_trait_object, DynClone};

use crate::context::Context;
use crate::message::Message;

pub trait ProcessState: Downcast + Debug {
    fn hash_with_dyn(&self, hasher: &mut dyn Hasher);
    fn eq_with_dyn(&self, other: &dyn ProcessState) -> bool;
}

impl_downcast!(ProcessState);

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct ProcessStateStub {}

impl<T: Hash + Eq + Debug + 'static> ProcessState for T {
    fn hash_with_dyn(&self, mut hasher: &mut dyn Hasher) {
        self.hash(&mut hasher);
    }

    fn eq_with_dyn(&self, other: &dyn ProcessState) -> bool {
        if let Some(other) = other.downcast_ref::<T>() {
            self.eq(other)
        } else {
            false
        }
    }
}

pub type StringProcessState = String;

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
    fn set_state(&mut self, _state: Box<dyn ProcessState>) {}
}

clone_trait_object!(Process);
