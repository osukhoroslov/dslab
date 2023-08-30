//! Tools for launching experiments with multiple environment configurations

use std::cell::RefCell;
use std::fs;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use dyn_clone::{clone_trait_object, DynClone};
use indexmap::map::IndexMap;
use log::Level;
use serde::{Deserialize, Serialize};
use sugars::{rc, refcell};
use threadpool::ThreadPool;

use dslab_core::Simulation;

use crate::core::config::exp_config::ExperimentConfig;
use crate::core::config::sim_config::SimulationConfig;
use crate::core::logger::{FileLogger, Logger, StdoutLogger};
use crate::simulation::CloudSimulation;

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(untagged)]
pub enum DictValue {
    String(String),
    Dict(IndexMap<String, DictValue>),
    Config(SimulationConfig),
}

/// Callbacks on simulation events
pub trait SimulationCallbacks: DynClone + Send {
    fn on_simulation_start(&mut self, _sim: Rc<RefCell<CloudSimulation>>) {
        // custom callback
    }

    // if returns false then the simulation is stopped
    fn on_step(&mut self, _sim: Rc<RefCell<CloudSimulation>>) -> bool {
        // custom callback
        true
    }

    /// returns experiment test case metrics
    fn on_simulation_finish(&mut self, _sim: Rc<RefCell<CloudSimulation>>) -> IndexMap<String, String> {
        // custom callback
        IndexMap::new()
    }
}

clone_trait_object!(SimulationCallbacks);

fn process_test_case(
    current_config: SimulationConfig,
    callbacks: &mut Box<dyn SimulationCallbacks>,
    log_file: Option<String>,
    log_level: Level,
) -> IndexMap<String, DictValue> {
    let logger: Box<dyn Logger> = if log_file.is_some() {
        Box::new(FileLogger::with_level(log_level))
    } else {
        Box::new(StdoutLogger::new())
    };

    let sim = Simulation::new(123);
    let cloud_sim = rc!(refcell!(CloudSimulation::with_logger(
        sim,
        current_config.clone(),
        logger
    )));
    callbacks.on_simulation_start(cloud_sim.clone());

    while cloud_sim.borrow_mut().current_time() < current_config.simulation_length {
        cloud_sim.borrow_mut().steps(1);
        let proceed = callbacks.on_step(cloud_sim.clone());
        if !proceed {
            break;
        }
    }

    let result = callbacks.on_simulation_finish(cloud_sim.clone());
    if log_file.is_some() {
        let save_result = cloud_sim.borrow().save_log(&log_file.clone().unwrap());
        match save_result {
            Ok(_) => println!("Log saved succesfully to file: {}", log_file.unwrap()),
            Err(e) => println!("Error while saving log: {e:?}"),
        }
    }

    let mut result_fmt = IndexMap::<String, DictValue>::new();
    for (key, value) in result {
        result_fmt.insert(key, DictValue::String(value));
    }

    result_fmt
}

pub struct Experiment {
    pub callbacks: Box<dyn SimulationCallbacks>,
    pub config: ExperimentConfig,
    pub threads: usize,
    pub log_dir: Option<String>,
    pub log_level: Level,
}

impl Experiment {
    pub fn new(
        callbacks: Box<dyn SimulationCallbacks>,
        config: ExperimentConfig,
        threads: usize,
        log_dir: Option<String>,
        log_level: Level,
    ) -> Self
    where
        Self: Sized,
    {
        if log_dir.is_some() {
            fs::create_dir_all(log_dir.clone().unwrap()).unwrap();
        }

        Self {
            callbacks,
            config,
            threads,
            log_dir,
            log_level,
        }
    }

    pub fn run(&mut self) {
        let results = Arc::new(Mutex::new(Vec::new()));
        let pool = ThreadPool::new(self.threads);
        let mut count = 1;

        while let Some(current_config) = self.config.get() {
            let config_info = format!("{0:?}", self.config);
            let callbacks = self.callbacks.clone();
            let log_level = self.log_level;

            let mut log_file = self.log_dir.clone();
            if log_file.is_some() {
                log_file = Some(format!("{}/log_{}.csv", log_file.unwrap(), count));
            }

            let results = results.clone();
            pool.execute(move || {
                println!();
                println!();
                println!("==== New test case ====");
                println!("{}", config_info);
                println!("=======================");
                println!();
                println!();

                let results = results.clone();
                let config_fmt = current_config.clone();
                let test_case_results =
                    process_test_case(current_config.clone(), &mut callbacks.clone(), log_file, log_level);

                let mut test_case_entry = IndexMap::<String, DictValue>::new();
                test_case_entry.insert("id".to_string(), DictValue::String(format!("{}", count)));
                test_case_entry.insert("config".to_string(), DictValue::Config(config_fmt));
                test_case_entry.insert("results".to_string(), DictValue::Dict(test_case_results));
                results.lock().unwrap().push(test_case_entry);
            });

            count += 1;
        }

        pool.join();
        let results = Arc::try_unwrap(results).unwrap().into_inner().unwrap();

        if self.log_dir.is_some() {
            let mut file = std::fs::File::create(format!("{}/results.json", self.log_dir.clone().unwrap())).unwrap();
            serde_json::to_writer_pretty(&mut file, &results).unwrap();
        }
    }
}
