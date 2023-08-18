use std::boxed::Box;
use std::cell::RefCell;
use std::collections::HashMap;
use std::ops::Deref;
use std::rc::Rc;
use std::str::FromStr;

use crate::config::parse_options;
use crate::function::Application;
use crate::host::Host;
use crate::scheduler::ApplicationHasher;

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

/// LocalityBasedDeployer picks a host based on application hash (see also `LocalityBasedScheduler` in `scheduler.rs`).
/// In case host number `i` can't deploy, the deployer considers host number `(i + step) % hosts.len()`.
pub struct LocalityBasedDeployer {
    hasher: ApplicationHasher,
    step: usize,
}

impl LocalityBasedDeployer {
    pub fn new(hasher: Option<ApplicationHasher>, step: Option<usize>) -> Self {
        let f = hasher.unwrap_or_else(|| ApplicationHasher::new(Box::new(|a| a), "Identity".to_string()));
        let s = step.unwrap_or(1);
        Self { hasher: f, step: s }
    }

    pub fn from_options_map(options: &HashMap<String, String>) -> Self {
        let hasher =
            ApplicationHasher::from_str(options.get("hasher").map(|a| a.deref()).unwrap_or("Identity")).unwrap();
        let step = options.get("step").map(|s| s.parse::<usize>().unwrap());
        Self::new(Some(hasher), step)
    }
}

impl IdleDeployer for LocalityBasedDeployer {
    fn deploy(&mut self, app: &Application, hosts: &[Rc<RefCell<Host>>]) -> Option<usize> {
        let start_idx = (self.hasher.hash(app.id as u64) % (hosts.len() as u64)) as usize;
        let mut idx = start_idx;
        loop {
            if hosts[idx].borrow().can_allocate(app.get_resources()) {
                return Some(idx);
            }
            idx = (idx + self.step) % hosts.len();
            if idx == start_idx {
                return None;
            }
        }
    }

    fn to_string(&self) -> String {
        format!(
            "LocalityBasedDeployer[hasher=[{}],step={}]",
            &self.hasher.name, self.step
        )
    }
}

pub fn default_idle_deployer_resolver(s: &str) -> Box<dyn IdleDeployer> {
    if s.len() >= 23 && &s[0..22] == "LocalityBasedDeployer[" && s.ends_with(']') {
        let opts = parse_options(&s[22..s.len() - 1]);
        return Box::new(LocalityBasedDeployer::from_options_map(&opts));
    }
    if s == "BasicDeployer" {
        return Box::new(BasicDeployer {});
    }
    panic!("Can't resolve: {}", s);
}
