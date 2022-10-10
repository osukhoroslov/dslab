use std::boxed::Box;

use crate::coldstart::{ColdStartPolicy, FixedTimeColdStartPolicy};
use crate::deployer::{BasicDeployer, IdleDeployer};
use crate::invoker::{BasicInvoker, Invoker};
use crate::scheduler::{BasicScheduler, Scheduler};

pub struct HostConfig {
    pub invoker: Box<dyn Invoker>,
    pub resources: Vec<(String, u64)>,
    pub cores: u32,
}

impl Default for HostConfig {
    fn default() -> Self {
        Self {
            invoker: Box::new(BasicInvoker::new()),
            resources: Vec::new(),
            cores: 1,
        }
    }
}

/// This is simulation config. It implements Default trait (see below) so that you can create
/// default config and change only the fields you need.
pub struct Config {
    pub coldstart_policy: Box<dyn ColdStartPolicy>,
    /// This field allows you to disable CPU contention (see cpu.rs).
    /// It may improve runtime at the cost of accuracy.
    pub disable_contention: bool,
    pub idle_deployer: Box<dyn IdleDeployer>,
    pub scheduler: Box<dyn Scheduler>,
    pub hosts: Vec<HostConfig>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            coldstart_policy: Box::new(FixedTimeColdStartPolicy::new(0.0, 0.0)),
            disable_contention: false,
            idle_deployer: Box::new(BasicDeployer {}),
            scheduler: Box::new(BasicScheduler {}),
            hosts: Vec::new(),
        }
    }
}
