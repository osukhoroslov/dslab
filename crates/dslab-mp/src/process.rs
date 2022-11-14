use crate::context::Context;
use crate::message::Message;

pub trait Process {
    fn on_message(&mut self, msg: Message, from: String, ctx: &mut Context);

    fn on_local_message(&mut self, msg: Message, ctx: &mut Context);

    fn on_timer(&mut self, timer: String, ctx: &mut Context);

    fn max_size(&mut self) -> u64 {
        0
    }
}
