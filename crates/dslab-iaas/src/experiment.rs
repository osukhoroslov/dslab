//! Tools for running experiments with multiple simulation runs.

use std::fs;
use std::fs::File;
use std::sync::{Arc, Mutex};

use dyn_clone::{clone_trait_object, DynClone};
use indexmap::map::IndexMap;
use log::Level;
use serde::{Deserialize, Serialize};
use threadpool::ThreadPool;

use simcore::Simulation;

use crate::core::config::exp_config::ExperimentConfig;
use crate::core::config::sim_config::SimulationConfig;
use crate::core::logger::{FileLogger, Logger, StdoutLogger};
use crate::simulation::CloudSimulation;

/// Trait for implementing custom callbacks for simulation runs within an experiment.
pub trait SimulationCallbacks: DynClone + Send {
    /// Runs before starting a simulation run.
    fn on_simulation_start(&mut self, _sim: &mut CloudSimulation) {}

    /// Runs on each step of a simulation run, returns false if the simulation must be stopped.
    fn on_step(&mut self, _sim: &mut CloudSimulation) -> bool {
        true
    }

    /// Runs upon the completion of a simulation run, returns results of this run.
    fn on_simulation_finish(&mut self, _sim: &mut CloudSimulation) -> IndexMap<String, String> {
        IndexMap::new()
    }
}

clone_trait_object!(SimulationCallbacks);

/// Implements execution of experiment.
pub struct Experiment {
    pub config: ExperimentConfig,
    pub callbacks: Box<dyn SimulationCallbacks>,
    pub log_dir: Option<String>,
    pub log_level: Level,
}

impl Experiment {
    pub fn new(
        config: ExperimentConfig,
        callbacks: Box<dyn SimulationCallbacks>,
        log_dir: Option<String>,
        log_level: Level,
    ) -> Self
    where
        Self: Sized,
    {
        if let Some(dir) = log_dir.clone() {
            fs::create_dir_all(dir).unwrap();
        }

        Self {
            config,
            callbacks,
            log_dir,
            log_level,
        }
    }

    /// Runs the experiment using the specified number of threads.
    pub fn run(&mut self, num_threads: usize) {
        let results = Arc::new(Mutex::new(Vec::new()));
        let pool = ThreadPool::new(num_threads);
        let mut run_id: usize = 1;

        while let Some(run_config) = self.config.get_next_run() {
            let config_info = format!("{0:?}", self.config);
            let mut callbacks = self.callbacks.clone();
            let log_level = self.log_level;
            let log_file = self.log_dir.clone().map(|dir| format!("{}/log_{}.csv", dir, run_id));
            let results = results.clone();

            pool.execute(move || {
                println!("RUN {}: {}", run_id, config_info);
                let run_results = run_simulation(run_id, run_config.clone(), &mut callbacks, log_file, log_level);

                let mut run_entry = IndexMap::<String, DictValue>::new();
                run_entry.insert("id".to_string(), DictValue::String(format!("{}", run_id)));
                run_entry.insert("config".to_string(), DictValue::Config(run_config));
                run_entry.insert("results".to_string(), DictValue::StringDict(run_results));
                results.lock().unwrap().push(run_entry);
            });

            run_id += 1;
        }

        pool.join();
        let results = Arc::try_unwrap(results).unwrap().into_inner().unwrap();

        if let Some(dir) = self.log_dir.clone() {
            let mut file = File::create(format!("{}/results.json", dir)).unwrap();
            serde_json::to_writer_pretty(&mut file, &results).unwrap();
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(untagged)]
enum DictValue {
    String(String),
    Dict(IndexMap<String, DictValue>),
    StringDict(IndexMap<String, String>),
    Config(SimulationConfig),
}

fn run_simulation(
    run_id: usize,
    config: SimulationConfig,
    callbacks: &mut Box<dyn SimulationCallbacks>,
    log_file: Option<String>,
    log_level: Level,
) -> IndexMap<String, String> {
    let logger: Box<dyn Logger> = if log_file.is_some() {
        Box::new(FileLogger::with_level(log_level))
    } else {
        Box::new(StdoutLogger::new())
    };

    let sim = Simulation::new(123);
    let mut cloud_sim = CloudSimulation::with_logger(sim, config.clone(), logger);
    callbacks.on_simulation_start(&mut cloud_sim);

    while cloud_sim.current_time() <= config.simulation_length {
        cloud_sim.steps(1);
        if !callbacks.on_step(&mut cloud_sim) {
            break;
        }
    }

    if let Some(log_file) = log_file {
        let save_result = cloud_sim.save_log(&log_file);
        match save_result {
            Ok(_) => println!("Log for run {run_id} saved successfully to file: {log_file}"),
            Err(e) => println!("Error while saving log for run {run_id}: {e:?}"),
        }
    }

    callbacks.on_simulation_finish(&mut cloud_sim)
}
