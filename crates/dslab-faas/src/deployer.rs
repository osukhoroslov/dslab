use std::cell::RefCell;
use std::rc::Rc;

use crate::function::Application;
use crate::host::Host;

/// IdleDeployer chooses an invoker to deploy new idle container on. Used for prewarm.
pub trait IdleDeployer {
    fn deploy(&mut self, app: &Application, hosts: &[Rc<RefCell<Host>>]) -> Option<usize>;

    fn get_name(&self) -> String {
        "STUB DEPLOYER NAME".to_string()
    }
}

/// BasicDeployer deploys new container on the first host with enough resources.
pub struct BasicDeployer {}

impl IdleDeployer for BasicDeployer {
    fn deploy(&mut self, app: &Application, hosts: &[Rc<RefCell<Host>>]) -> Option<usize> {
        for (i, host) in hosts.iter().enumerate() {
            if host.borrow().can_allocate(app.get_resources()) {
                return Some(i);
            }
        }
        None
    }

    fn get_name(&self) -> String {
        "Basic Deployer".to_string()
    }
}
