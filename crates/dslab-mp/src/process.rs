use std::fmt::Debug;
use std::rc::Rc;

use downcast_rs::{impl_downcast, Downcast};
use dyn_clone::{clone_trait_object, DynClone};

use crate::context::Context;
use crate::message::Message;

pub trait ProcessState: Downcast + Debug {
    fn hash(&self) -> u64;
}

impl_downcast!(ProcessState);

#[derive(Debug, Clone)]
struct ProcessStateStub {}

impl ProcessState for ProcessStateStub {
    fn hash(&self) -> u64 {
        0
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
    fn state(&self) -> Rc<dyn ProcessState> {
        Rc::new(ProcessStateStub {})
    }

    /// Restores the process state.
    fn set_state(&self, _state: Rc<dyn ProcessState>) {}
}

clone_trait_object!(Process);
