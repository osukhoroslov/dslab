use crate::context::Context;
use crate::message::Message;
use std::cell::RefCell;
use std::rc::Rc;

pub trait Process: ProcessConvertHelper {
    fn on_message(&mut self, msg: Message, from: String, ctx: &mut Context);

    fn on_local_message(&mut self, msg: Message, ctx: &mut Context);

    fn on_timer(&mut self, timer: String, ctx: &mut Context);

    fn max_size(&mut self) -> u64 {
        0
    }
}

pub trait ProcessConvertHelper {
    fn box_to_rc(self: Box<Self>) -> Rc<RefCell<dyn Process>>;
}

impl<T: Process + 'static> ProcessConvertHelper for T {
    fn box_to_rc(self: Box<Self>) -> Rc<RefCell<dyn Process>> {
        Rc::new(RefCell::new(*self))
    }
}
