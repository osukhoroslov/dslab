use std::cell::RefCell;
use std::rc::Rc;

use crate::function::Application;
use crate::host::Host;

/*
 * Scheduler chooses an invoker
 * to run new invocation of some function
 * from given application.
 */
pub trait Scheduler {
    fn select_host(&mut self, app: &Application, hosts: &[Rc<RefCell<Host>>]) -> usize;

    fn get_name(&self) -> String {
        "STUB SCHEDULER NAME".to_string()
    }
}

/* BasicScheduler chooses the first invoker
 * that can hotstart the invocation,
 * otherwise it chooses the first invoker that can deploy the container.
 */
pub struct BasicScheduler {}

impl Scheduler for BasicScheduler {
    fn select_host(&mut self, app: &Application, hosts: &[Rc<RefCell<Host>>]) -> usize {
        for (i, host) in hosts.iter().enumerate() {
            if host.borrow().can_invoke(app, true) {
                return i;
            }
        }
        for (i, host) in hosts.iter().enumerate() {
            if host.borrow().can_allocate(app.get_resources()) {
                return i;
            }
        }
        0
    }

    fn get_name(&self) -> String {
        "BasicScheduler".to_string()
    }
}
