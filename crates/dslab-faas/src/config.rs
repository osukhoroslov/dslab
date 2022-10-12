use std::boxed::Box;
use std::fs::File;
use std::path::Path;

use serde::{Deserialize, Serialize};

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

#[derive(Clone, Serialize, Deserialize)]
pub struct RawResource {
    pub name: String,
    pub quantity: u64,
}

fn default_one() -> u32 {
    1
}

#[derive(Clone, Serialize, Deserialize)]
pub struct RawHostConfig {
    #[serde(default)]
    pub invoker: String,
    pub resources: Vec<RawResource>,
    pub cores: u32,
    #[serde(default = "default_one")]
    pub count: u32,
}

/// YAML-serializeable config
#[derive(Clone, Serialize, Deserialize)]
pub struct RawConfig {
    #[serde(default)]
    pub coldstart_policy: String,
    #[serde(default)]
    pub disable_contention: bool,
    #[serde(default)]
    pub idle_deployer: String,
    #[serde(default)]
    pub scheduler: String,
    #[serde(default)]
    pub hosts: Vec<RawHostConfig>,
}

pub fn stub_coldstart_policy_resolver(_s: &str) -> Box<dyn ColdStartPolicy> {
    Box::new(FixedTimeColdStartPolicy::new(0.0, 0.0))
}

pub fn stub_idle_deployer_resolver(_s: &str) -> Box<dyn IdleDeployer> {
    Box::new(BasicDeployer {})
}

pub fn stub_scheduler_resolver(_s: &str) -> Box<dyn Scheduler> {
    Box::new(BasicScheduler {})
}

pub fn stub_invoker_resolver(_s: &str) -> Box<dyn Invoker> {
    Box::new(BasicInvoker::new())
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

impl Config {
    pub fn from_raw(
        raw: RawConfig,
        coldstart_policy_resolver: &dyn Fn(&str) -> Box<dyn ColdStartPolicy>,
        idle_deployer_resolver: &dyn Fn(&str) -> Box<dyn IdleDeployer>,
        scheduler_resolver: &dyn Fn(&str) -> Box<dyn Scheduler>,
        invoker_resolver: &dyn Fn(&str) -> Box<dyn Invoker>,
    ) -> Self {
        let mut me: Self = Default::default();
        if !raw.coldstart_policy.is_empty() {
            me.coldstart_policy = coldstart_policy_resolver(&raw.coldstart_policy);
        }
        if !raw.idle_deployer.is_empty() {
            me.idle_deployer = idle_deployer_resolver(&raw.idle_deployer);
        }
        if !raw.scheduler.is_empty() {
            me.scheduler = scheduler_resolver(&raw.scheduler);
        }
        for host in raw.hosts {
            let mut resources = Vec::with_capacity(host.resources.len());
            for item in host.resources {
                resources.push((item.name, item.quantity));
            }
            for _ in 0..host.count {
                let mut curr: HostConfig = Default::default();
                curr.resources = resources.clone();
                curr.cores = host.cores;
                if !host.invoker.is_empty() {
                    curr.invoker = invoker_resolver(&host.invoker);
                }
                me.hosts.push(curr);
            }
        }
        me
    }

    pub fn from_yaml(
        path: &Path,
        coldstart_policy_resolver: &dyn Fn(&str) -> Box<dyn ColdStartPolicy>,
        idle_deployer_resolver: &dyn Fn(&str) -> Box<dyn IdleDeployer>,
        scheduler_resolver: &dyn Fn(&str) -> Box<dyn Scheduler>,
        invoker_resolver: &dyn Fn(&str) -> Box<dyn Invoker>,
    ) -> Self {
        let f = File::open(path).unwrap();
        Self::from_raw(
            serde_yaml::from_reader(f).unwrap(),
            coldstart_policy_resolver,
            idle_deployer_resolver,
            scheduler_resolver,
            invoker_resolver,
        )
    }
}
