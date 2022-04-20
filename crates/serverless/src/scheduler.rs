use std::cell::RefCell;
use std::rc::Rc;

use crate::function::Application;
use crate::invoker::Invoker;

/*
 * Scheduler chooses an invoker
 * to run new invocation of some function
 * from given application.
 */
pub trait Scheduler {
    fn select_invoker(&mut self, app: &Application, invokers: &Vec<Rc<RefCell<Invoker>>>) -> usize;
}

/* BasicScheduler chooses the first invoker
 * that can hotstart the invocation,
 * otherwise it chooses the first invoker that can deploy the container.
 */
pub struct BasicScheduler {}

impl Scheduler for BasicScheduler {
    fn select_invoker(&mut self, app: &Application, invokers: &Vec<Rc<RefCell<Invoker>>>) -> usize {
        for (i, invoker) in invokers.iter().enumerate() {
            if invoker.borrow().can_invoke(app) {
                return i;
            }
        }
        for (i, invoker) in invokers.iter().enumerate() {
            if invoker.borrow().can_allocate(app.get_resources()) {
                return i;
            }
        }
        0
    }
}
