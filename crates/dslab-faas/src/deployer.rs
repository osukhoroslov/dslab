use std::boxed::Box;
use std::cell::RefCell;
use std::rc::Rc;

use crate::function::Application;
use crate::host::Host;

/// IdleDeployer chooses an invoker to deploy new idle container on. Used for prewarm.
pub trait IdleDeployer {
    fn deploy(&mut self, app: &Application, hosts: &[Rc<RefCell<Host>>]) -> Option<usize>;

    fn to_string(&self) -> String {
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

    fn to_string(&self) -> String {
        "BasicDeployer".to_string()
    }
}

pub fn default_idle_deployer_resolver(s: &str) -> Box<dyn IdleDeployer> {
    if s == "BasicDeployer" {
        Box::new(BasicDeployer {})
    } else {
        panic!("Can't resolve: {}", s);
    }
}
