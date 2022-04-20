use std::cell::RefCell;
use std::rc::Rc;

use crate::function::Application;
use crate::invoker::Invoker;

/*
 * IdleDeployer chooses an invoker to deploy
 * new idle container on. Used for prewarm.
 */
pub trait IdleDeployer {
    fn deploy(&mut self, app: &Application, invokers: &Vec<Rc<RefCell<Invoker>>>) -> Option<usize>;
}

// BasicDeployer deploys new container on
// the first host with enough resources
pub struct BasicDeployer {}

impl IdleDeployer for BasicDeployer {
    fn deploy(&mut self, app: &Application, invokers: &Vec<Rc<RefCell<Invoker>>>) -> Option<usize> {
        for (i, invoker) in invokers.iter().enumerate() {
            if invoker.borrow().can_allocate(app.get_resources()) {
                return Some(i);
            }
        }
        None
    }
}
