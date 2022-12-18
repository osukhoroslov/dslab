use crate::context::Context;
use crate::message::Message;

pub trait Process {

    /// This method is called when a message is received.
    fn on_message(&mut self, msg: Message, from: String, ctx: &mut Context);

    /// This method is called when a __local_ message is received.
    fn on_local_message(&mut self, msg: Message, ctx: &mut Context);

    /// This method is called when a timer fires.
    fn on_timer(&mut self, timer: String, ctx: &mut Context);

    /// This function returns size of process inner data at current moment.
    fn max_size(&mut self) -> u64 {
        0
    }

    /// This function returns string representation of process state.
    fn state(&self) -> String;

    /// This function restores process by it's serialized state.
    fn set_state(&self, data: &String);
}
