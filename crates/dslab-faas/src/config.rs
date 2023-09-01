//! Experiment configuration (YAML-serializable).

use std::boxed::Box;
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::coldstart::{default_coldstart_policy_resolver, ColdStartPolicy, FixedTimeColdStartPolicy};
use crate::cpu::{default_cpu_policy_resolver, ContendedCpuPolicy, CpuPolicy};
use crate::deployer::{default_idle_deployer_resolver, BasicDeployer, IdleDeployer};
use crate::invoker::{default_invoker_resolver, FIFOInvoker, Invoker};
use crate::parallel::{ParallelConfig, ParallelHostConfig};
use crate::scheduler::{default_scheduler_resolver, BasicScheduler, Scheduler};

/// Describes a host in the simulation.
pub struct HostConfig {
    /// [`crate::invoker::Invoker`] implementation.
    pub invoker: Box<dyn Invoker>,
    /// Host resources.
    pub resources: Vec<(String, u64)>,
    /// Host CPU cores.
    pub cores: u32,
}

impl From<ParallelHostConfig> for HostConfig {
    fn from(value: ParallelHostConfig) -> Self {
        Self {
            invoker: value.invoker,
            resources: value.resources,
            cores: value.cores,
        }
    }
}

impl Default for HostConfig {
    fn default() -> Self {
        Self {
            invoker: Box::new(FIFOInvoker::new()),
            resources: Vec::new(),
            cores: 1,
        }
    }
}

impl From<ParallelConfig> for Config {
    fn from(value: ParallelConfig) -> Self {
        let mut hosts = value.hosts;
        Self {
            coldstart_policy: value.coldstart_policy,
            cpu_policy: value.cpu_policy,
            idle_deployer: value.idle_deployer,
            scheduler: value.scheduler,
            hosts: hosts.drain(..).map(HostConfig::from).collect(),
        }
    }
}

/// Describes a resource in the simulation.
#[derive(Clone, Serialize, Deserialize)]
pub struct RawResource {
    /// Resource name.
    pub name: String,
    /// Resource quantity.
    pub quantity: u64,
}

fn default_one() -> u32 {
    1
}

/// Similar to [`HostConfig`], but is YAML-serializable.
#[derive(Clone, Serialize, Deserialize)]
pub struct RawHostConfig {
    /// [`crate::invoker::Invoker`] name.
    #[serde(default)]
    pub invoker: String,
    /// Raw resources.
    pub resources: Vec<RawResource>,
    /// Host CPU cores.
    pub cores: u32,
    /// Number of such hosts in the system.
    #[serde(default = "default_one")]
    pub count: u32,
}

/// YAML-serializable config
#[derive(Clone, Serialize, Deserialize)]
pub struct RawConfig {
    /// [`crate::coldstart::ColdStartPolicy`] name.
    #[serde(default)]
    pub coldstart_policy: String,
    /// [`crate::cpu::CpuPolicy`] name.
    #[serde(default)]
    pub cpu_policy: String,
    /// [`crate::deployer::IdleDeployer`] name.
    #[serde(default)]
    pub idle_deployer: String,
    /// [`crate::scheduler::Scheduler`] name.
    #[serde(default)]
    pub scheduler: String,
    /// Raw host data.
    #[serde(default)]
    pub hosts: Vec<RawHostConfig>,
}

/// Parses map with options from string.
pub fn parse_options(s: &str) -> HashMap<String, String> {
    let mut ans = HashMap::new();
    for t in s.split(',') {
        let val = t.split_once('=');
        if let Some((l, r)) = val {
            ans.insert(l.to_string(), r.to_string());
        }
    }
    ans
}

/// Functions that create algorithm implementation from a string containing algorithm name and options.
pub struct ConfigParamResolvers {
    /// Creates [`crate::coldstart::ColdStartPolicy`] from a string.
    pub coldstart_policy_resolver: Box<dyn Fn(&str) -> Box<dyn ColdStartPolicy> + Send + Sync>,
    /// Creates [`crate::cpu::CpuPolicy`] from a string.
    pub cpu_policy_resolver: Box<dyn Fn(&str) -> Box<dyn CpuPolicy> + Send + Sync>,
    /// Creates [`crate::deployer::IdleDeployer`] from a string.
    pub idle_deployer_resolver: Box<dyn Fn(&str) -> Box<dyn IdleDeployer> + Send + Sync>,
    /// Creates [`crate::scheduler::Scheduler`] from a string.
    pub scheduler_resolver: Box<dyn Fn(&str) -> Box<dyn Scheduler> + Send + Sync>,
    /// Creates [`crate::invoker::Invoker`] from a string.
    pub invoker_resolver: Box<dyn Fn(&str) -> Box<dyn Invoker> + Send + Sync>,
}

impl Default for ConfigParamResolvers {
    fn default() -> Self {
        Self {
            coldstart_policy_resolver: Box::new(default_coldstart_policy_resolver),
            cpu_policy_resolver: Box::new(default_cpu_policy_resolver),
            idle_deployer_resolver: Box::new(default_idle_deployer_resolver),
            scheduler_resolver: Box::new(default_scheduler_resolver),
            invoker_resolver: Box::new(default_invoker_resolver),
        }
    }
}

/// Simulation config. It implements Default trait so that you can create default config and change only the fields you need.
pub struct Config {
    /// [`crate::coldstart::ColdStartPolicy`] implementation.
    pub coldstart_policy: Box<dyn ColdStartPolicy>,
    /// [`crate::cpu::CpuPolicy`] implementation.
    pub cpu_policy: Box<dyn CpuPolicy>,
    /// [`crate::deployer::IdleDeployer`] implementation.
    pub idle_deployer: Box<dyn IdleDeployer>,
    /// [`crate::scheduler::Scheduler`] implementation.
    pub scheduler: Box<dyn Scheduler>,
    /// Host data.
    pub hosts: Vec<HostConfig>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            coldstart_policy: Box::new(FixedTimeColdStartPolicy::new(0.0, 0.0, false)),
            cpu_policy: Box::<ContendedCpuPolicy>::default(),
            idle_deployer: Box::new(BasicDeployer {}),
            scheduler: Box::new(BasicScheduler {}),
            hosts: Vec::new(),
        }
    }
}

impl Config {
    /// Creates Config from RawConfig using provided resolvers.
    pub fn from_raw(raw: RawConfig, resolvers: ConfigParamResolvers) -> Self {
        Self::from_raw_split_resolvers(
            raw,
            resolvers.coldstart_policy_resolver.as_ref(),
            resolvers.cpu_policy_resolver.as_ref(),
            resolvers.idle_deployer_resolver.as_ref(),
            resolvers.scheduler_resolver.as_ref(),
            resolvers.invoker_resolver.as_ref(),
        )
    }

    /// Similar to [`Self::from_raw`], but takes resolvers as separate functions.
    pub fn from_raw_split_resolvers(
        raw: RawConfig,
        coldstart_policy_resolver: &(dyn Fn(&str) -> Box<dyn ColdStartPolicy> + Send + Sync),
        cpu_policy_resolver: &(dyn Fn(&str) -> Box<dyn CpuPolicy> + Send + Sync),
        idle_deployer_resolver: &(dyn Fn(&str) -> Box<dyn IdleDeployer> + Send + Sync),
        scheduler_resolver: &(dyn Fn(&str) -> Box<dyn Scheduler> + Send + Sync),
        invoker_resolver: &(dyn Fn(&str) -> Box<dyn Invoker> + Send + Sync),
    ) -> Self {
        let mut me: Self = Default::default();
        if !raw.coldstart_policy.is_empty() {
            me.coldstart_policy = coldstart_policy_resolver(&raw.coldstart_policy);
        }
        if !raw.cpu_policy.is_empty() {
            me.cpu_policy = cpu_policy_resolver(&raw.cpu_policy);
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
                let invoker = if !host.invoker.is_empty() {
                    invoker_resolver(&host.invoker)
                } else {
                    Box::new(FIFOInvoker::new())
                };
                let curr = HostConfig {
                    invoker,
                    resources: resources.clone(),
                    cores: host.cores,
                };
                me.hosts.push(curr);
            }
        }
        me
    }

    /// Creates Config from YAML file using provided resolvers.
    pub fn from_yaml(path: &Path, resolvers: ConfigParamResolvers) -> Self {
        let f = File::open(path).unwrap();
        Self::from_raw(serde_yaml::from_reader(f).unwrap(), resolvers)
    }
}
