//! Utilities for running multiple experiments in parallel.
#![allow(clippy::type_complexity)]

use std::boxed::Box;
use std::convert::Into;
use std::fs::File;
use std::path::Path;
use std::sync::{mpsc::channel, Arc};

use itertools::izip;
use threadpool::ThreadPool;

use simcore::simulation::Simulation;

use crate::coldstart::{ColdStartPolicy, FixedTimeColdStartPolicy};
use crate::config::{Config, ConfigParamResolvers, RawConfig};
use crate::cpu::{ContendedCpuPolicy, CpuPolicy};
use crate::deployer::{BasicDeployer, IdleDeployer};
use crate::invoker::{FIFOInvoker, Invoker};
use crate::scheduler::{BasicScheduler, Scheduler};
use crate::simulation::ServerlessSimulation;
use crate::stats::Stats;
use crate::trace::Trace;

/// Similar to [`crate::config::HostConfig`], but only accepts invokers with `Send` trait.
pub struct ParallelHostConfig {
    /// [`crate::invoker::Invoker`] implementation.
    pub invoker: Box<dyn Invoker + Send>,
    /// Host resources.
    pub resources: Vec<(String, u64)>,
    /// Host CPU cores.
    pub cores: u32,
}

impl Default for ParallelHostConfig {
    fn default() -> Self {
        Self {
            invoker: Box::new(FIFOInvoker::new()),
            resources: Vec::new(),
            cores: 1,
        }
    }
}

/// Similar to [`crate::config::Config`], but ensures that all simulation components implement `Send` trait.
pub struct ParallelConfig {
    /// [`crate::coldstart::ColdStartPolicy`] implementation.
    pub coldstart_policy: Box<dyn ColdStartPolicy + Send>,
    /// [`crate::cpu::CpuPolicy`] implementation.
    pub cpu_policy: Box<dyn CpuPolicy + Send>,
    /// [`crate::deployer::IdleDeployer`] implementation.
    pub idle_deployer: Box<dyn IdleDeployer + Send>,
    /// [`crate::scheduler::Scheduler`] implementation.
    pub scheduler: Box<dyn Scheduler + Send>,
    /// Host configuration data.
    pub hosts: Vec<ParallelHostConfig>,
}

impl Default for ParallelConfig {
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

/// Runs parallel simulations in a thread pool with `n_workers` worker threads.
pub fn parallel_simulation_n_workers(
    mut configs: Vec<ParallelConfig>,
    mut traces: Vec<Box<dyn Trace + Send + Sync>>,
    mut seeds: Vec<u64>,
    n_workers: usize,
) -> Vec<Stats> {
    assert!(
        traces.len() == 1 || traces.len() == configs.len(),
        "There should be one trace for each config or one trace for all configs."
    );
    assert!(
        seeds.len() == 1 || seeds.len() == configs.len(),
        "There should be one seed for each config or one seed for all configs."
    );
    let mut traces_arc: Vec<Arc<dyn Trace + Send + Sync>> = Vec::with_capacity(configs.len());
    if traces.len() == 1 {
        let trace = traces.drain(..).next().unwrap();
        let ptr: Arc<dyn Trace + Send + Sync> = Arc::from(trace);
        for _ in 0..configs.len() {
            traces_arc.push(ptr.clone())
        }
    } else {
        for trace in traces.drain(..) {
            traces_arc.push(Arc::from(trace));
        }
    }
    if seeds.len() == 1 {
        let seed = seeds[0];
        seeds = vec![seed; configs.len()];
    }
    let pool = ThreadPool::new(n_workers);
    let (tx, rx) = channel();
    let len = configs.len();
    for (id, config, trace, seed) in izip!(0..len, configs.drain(..), traces_arc.drain(..), seeds.drain(..)) {
        let tx = tx.clone();
        pool.execute(move || {
            let mut sim = ServerlessSimulation::new(Simulation::new(seed), config.into());
            sim.load_trace(trace.as_ref());
            sim.step_until_no_events();
            tx.send((id, sim.stats())).unwrap();
        });
    }
    let mut results: Vec<_> = rx.iter().take(len).collect();
    results.sort_by_key(|x| x.0);
    results.drain(..).map(|x| x.1).collect()
}

/// Runs parallel simulations in a thread pool with a separate worker for each config.
pub fn parallel_simulation(
    configs: Vec<ParallelConfig>,
    traces: Vec<Box<dyn Trace + Send + Sync>>,
    seeds: Vec<u64>,
) -> Vec<Stats> {
    let n_workers = configs.len();
    parallel_simulation_n_workers(configs, traces, seeds, n_workers)
}

/// Similar to [`parallel_simulation_n_workers`], but for raw configs.
pub fn parallel_simulation_raw_n_workers(
    mut configs: Vec<RawConfig>,
    resolvers: ConfigParamResolvers,
    mut traces: Vec<Box<dyn Trace + Send + Sync>>,
    mut seeds: Vec<u64>,
    n_workers: usize,
) -> Vec<Stats> {
    assert!(
        traces.len() == 1 || traces.len() == configs.len(),
        "There should be one trace for each config or one trace for all configs."
    );
    assert!(
        seeds.len() == 1 || seeds.len() == configs.len(),
        "There should be one seed for each config or one seed for all configs."
    );
    let mut traces_arc: Vec<Arc<dyn Trace + Send + Sync>> = Vec::with_capacity(configs.len());
    if traces.len() == 1 {
        let trace = traces.drain(..).next().unwrap();
        let ptr: Arc<dyn Trace + Send + Sync> = Arc::from(trace);
        for _ in 0..configs.len() {
            traces_arc.push(ptr.clone())
        }
    } else {
        for trace in traces.drain(..) {
            traces_arc.push(Arc::from(trace));
        }
    }
    if seeds.len() == 1 {
        let seed = seeds[0];
        seeds = vec![seed; configs.len()];
    }
    let coldstart_policy_resolver1: Arc<dyn Fn(&str) -> Box<dyn ColdStartPolicy> + Send + Sync> =
        Arc::from(resolvers.coldstart_policy_resolver);
    let cpu_policy_resolver1: Arc<dyn Fn(&str) -> Box<dyn CpuPolicy> + Send + Sync> =
        Arc::from(resolvers.cpu_policy_resolver);
    let idle_deployer_resolver1: Arc<dyn Fn(&str) -> Box<dyn IdleDeployer> + Send + Sync> =
        Arc::from(resolvers.idle_deployer_resolver);
    let scheduler_resolver1: Arc<dyn Fn(&str) -> Box<dyn Scheduler> + Send + Sync> =
        Arc::from(resolvers.scheduler_resolver);
    let invoker_resolver1: Arc<dyn Fn(&str) -> Box<dyn Invoker> + Send + Sync> = Arc::from(resolvers.invoker_resolver);
    let pool = ThreadPool::new(n_workers);
    let (tx, rx) = channel();
    let len = configs.len();
    for (id, raw_config, trace, seed) in izip!(0..len, configs.drain(..), traces_arc.drain(..), seeds.drain(..)) {
        let tx = tx.clone();
        let coldstart_policy_resolver = coldstart_policy_resolver1.clone();
        let cpu_policy_resolver = cpu_policy_resolver1.clone();
        let idle_deployer_resolver = idle_deployer_resolver1.clone();
        let scheduler_resolver = scheduler_resolver1.clone();
        let invoker_resolver = invoker_resolver1.clone();
        pool.execute(move || {
            let config = Config::from_raw_split_resolvers(
                raw_config,
                coldstart_policy_resolver.as_ref(),
                cpu_policy_resolver.as_ref(),
                idle_deployer_resolver.as_ref(),
                scheduler_resolver.as_ref(),
                invoker_resolver.as_ref(),
            );
            let mut sim = ServerlessSimulation::new(Simulation::new(seed), config);
            sim.load_trace(trace.as_ref());
            sim.step_until_no_events();
            tx.send((id, sim.stats())).unwrap();
        });
    }
    let mut results: Vec<_> = rx.iter().take(len).collect();
    results.sort_by_key(|x| x.0);
    results.drain(..).map(|x| x.1).collect()
}

/// Similar to [`parallel_simulation`], but for raw configs.
pub fn parallel_simulation_raw(
    configs: Vec<RawConfig>,
    resolvers: ConfigParamResolvers,
    traces: Vec<Box<dyn Trace + Send + Sync>>,
    seeds: Vec<u64>,
) -> Vec<Stats> {
    let n_workers = configs.len();
    parallel_simulation_raw_n_workers(configs, resolvers, traces, seeds, n_workers)
}

/// Similar to [`parallel_simulation_n_workers`], but for YAML configs.
pub fn parallel_simulation_yaml_n_workers(
    configs: Vec<&Path>,
    resolvers: ConfigParamResolvers,
    traces: Vec<Box<dyn Trace + Send + Sync>>,
    seeds: Vec<u64>,
    n_workers: usize,
) -> Vec<Stats> {
    parallel_simulation_raw_n_workers(
        configs
            .iter()
            .map(|x| {
                let f = File::open(x).unwrap();
                serde_yaml::from_reader(f).unwrap()
            })
            .collect::<Vec<_>>(),
        resolvers,
        traces,
        seeds,
        n_workers,
    )
}

/// Similar to [`parallel_simulation`], but for YAML configs.
pub fn parallel_simulation_yaml(
    configs: Vec<&Path>,
    resolvers: ConfigParamResolvers,
    traces: Vec<Box<dyn Trace + Send + Sync>>,
    seeds: Vec<u64>,
) -> Vec<Stats> {
    let n_workers = configs.len();
    parallel_simulation_yaml_n_workers(configs, resolvers, traces, seeds, n_workers)
}
