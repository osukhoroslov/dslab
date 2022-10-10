use std::boxed::Box;
use std::convert::Into;
use std::sync::{mpsc::channel, Arc};

use itertools::izip;
use threadpool::ThreadPool;

use dslab_core::simulation::Simulation;

use crate::coldstart::{ColdStartPolicy, FixedTimeColdStartPolicy};
use crate::config::{Config, HostConfig};
use crate::deployer::{BasicDeployer, IdleDeployer};
use crate::invoker::{BasicInvoker, Invoker};
use crate::scheduler::{BasicScheduler, Scheduler};
use crate::simulation::ServerlessSimulation;
use crate::stats::Stats;
use crate::trace::Trace;

pub struct ParallelHostConfig {
    pub invoker: Box<dyn Invoker + Send>,
    pub resources: Vec<(String, u64)>,
    pub cores: u32,
}

impl Default for ParallelHostConfig {
    fn default() -> Self {
        Self {
            invoker: Box::new(BasicInvoker::new()),
            resources: Vec::new(),
            cores: 1,
        }
    }
}

impl Into<HostConfig> for ParallelHostConfig {
    fn into(self) -> HostConfig {
        HostConfig {
            invoker: self.invoker,
            resources: self.resources,
            cores: self.cores,
        }
    }
}

pub struct ParallelConfig {
    pub coldstart_policy: Box<dyn ColdStartPolicy + Send>,
    pub disable_contention: bool,
    pub idle_deployer: Box<dyn IdleDeployer + Send>,
    pub scheduler: Box<dyn Scheduler + Send>,
    pub hosts: Vec<ParallelHostConfig>,
}

impl Default for ParallelConfig {
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

impl Into<Config> for ParallelConfig {
    fn into(self) -> Config {
        let mut hosts = self.hosts;
        Config {
            coldstart_policy: self.coldstart_policy,
            disable_contention: self.disable_contention,
            idle_deployer: self.idle_deployer,
            scheduler: self.scheduler,
            hosts: hosts.drain(..).map(|x| x.into()).collect(),
        }
    }
}

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
            tx.send((id, sim.get_stats())).unwrap();
        });
    }
    let mut results: Vec<_> = rx.iter().take(len).collect();
    results.sort_by_key(|x| x.0);
    results.drain(..).map(|x| x.1).collect()
}

pub fn parallel_simulation(
    configs: Vec<ParallelConfig>,
    traces: Vec<Box<dyn Trace + Send + Sync>>,
    seeds: Vec<u64>,
) -> Vec<Stats> {
    let n_workers = configs.len();
    parallel_simulation_n_workers(configs, traces, seeds, n_workers)
}
