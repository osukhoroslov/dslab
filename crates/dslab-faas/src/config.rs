use std::boxed::Box;
use std::cell::RefCell;
use std::rc::Rc;

use crate::coldstart::{ColdStartPolicy, FixedTimeColdStartPolicy};
use crate::deployer::{BasicDeployer, IdleDeployer};
use crate::scheduler::{BasicScheduler, Scheduler};

/// This is simulation config. It implements Default trait (see below) so that you can create
/// default config and change only the fields you need.
pub struct Config {
    pub coldstart_policy: Rc<RefCell<dyn ColdStartPolicy>>,
    /// This field allows you to disable CPU contention (see cpu.rs).
    /// It may improve runtime at the cost of accuracy.
    pub disable_contention: bool,
    pub idle_deployer: Box<dyn IdleDeployer>,
    pub scheduler: Box<dyn Scheduler>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            coldstart_policy: Rc::new(RefCell::new(FixedTimeColdStartPolicy::new(0.0, 0.0))),
            disable_contention: false,
            idle_deployer: Box::new(BasicDeployer {}),
            scheduler: Box::new(BasicScheduler {}),
        }
    }
}
