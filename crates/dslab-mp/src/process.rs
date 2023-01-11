use crate::context::Context;
use crate::message::Message;

pub trait Process {
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
    fn state(&self) -> String {
        "".to_string()
    }

    /// Restores the process state by its string representation.
    fn set_state(&self, _data: &String) {}
}
