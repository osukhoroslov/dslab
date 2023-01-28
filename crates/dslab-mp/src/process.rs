use downcast_rs::{impl_downcast, Downcast};
use dyn_clone::{clone_trait_object, DynClone};
use std::fmt::Debug;

use crate::context::Context;
use crate::message::Message;

pub trait ProcessState: Downcast + Debug {}

impl_downcast!(ProcessState);

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

    /// Returns the string representation of process state.
    fn state(&self) -> Box<dyn ProcessState>;

    /// Restores the process state by its string representation.
    fn set_state(&self, state: Box<dyn ProcessState>);
}

clone_trait_object!(Process);
