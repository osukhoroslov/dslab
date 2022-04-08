use crate::function::Group;
use crate::invoker::Invoker;

use std::cell::RefCell;
use std::rc::Rc;

/*
 * Scheduler chooses an invoker
 * to run new invocation of some function
 * from given function group.
 */
pub trait Scheduler {
    fn select_invoker(&mut self, group: &Group, invokers: &Vec<Rc<RefCell<Invoker>>>) -> usize;
}

/* BasicScheduler chooses the first invoker
 * that can hotstart the invocation,
 * otherwise it chooses the first invoker that can deploy the container.
 */
pub struct BasicScheduler {}

impl Scheduler for BasicScheduler {
    fn select_invoker(&mut self, group: &Group, invokers: &Vec<Rc<RefCell<Invoker>>>) -> usize {
        for (i, invoker) in invokers.iter().enumerate() {
            if invoker.borrow().can_invoke(group) {
                return i;
            }
        }
        for (i, invoker) in invokers.iter().enumerate() {
            if invoker.borrow().can_allocate(group.get_resources()) {
                return i;
            }
        }
        0
    }
}
