//! Process trait and related types.

use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::rc::Rc;

use downcast_rs::{impl_downcast, Downcast};
use dyn_clone::{clone_trait_object, DynClone};

use crate::context::Context;
use crate::message::Message;

/// A trait for process implementations.
pub trait Process: DynClone {
    /// Called when a message is received.
    fn on_message(&mut self, msg: Message, from: String, ctx: &mut Context) -> Result<(), String>;

    /// Called when a _local_ message is received.
    fn on_local_message(&mut self, msg: Message, ctx: &mut Context) -> Result<(), String>;

    /// Called when a timer fires.
    fn on_timer(&mut self, timer: String, ctx: &mut Context) -> Result<(), String>;

    /// Returns the maximum size of process inner data observed so far.
    fn max_size(&mut self) -> u64 {
        0
    }

    /// Returns the process state.
    fn state(&self) -> Result<Rc<dyn ProcessState>, String> {
        Ok(Rc::new(ProcessStateStub {}))
    }

    /// Restores the process state.
    fn set_state(&mut self, _state: Rc<dyn ProcessState>) -> Result<(), String> {
        Ok(())
    }
}

clone_trait_object!(Process);

/// A trait for implementations of process state.
pub trait ProcessState: Downcast + Debug {
    /// Computes a hash of process state using the passed hasher.
    fn hash_with_dyn(&self, hasher: &mut dyn Hasher);
    /// Tests for `self` and `other` values to be equal.
    fn eq_with_dyn(&self, other: &dyn ProcessState) -> bool;
}

impl_downcast!(ProcessState);

/// Empty process state.
#[derive(Debug, Hash, Eq, PartialEq)]
pub struct ProcessStateStub {}

/// Process state encoded by a string.
pub type StringProcessState = String;

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
