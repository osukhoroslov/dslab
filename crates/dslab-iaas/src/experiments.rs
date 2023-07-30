//! Tools for launching experiments with multiple environment configurations

use std::cell::RefCell;
use std::rc::Rc;

use dyn_clone::{clone_trait_object, DynClone};
use sugars::{rc, refcell};
use threadpool::ThreadPool;

use dslab_core::Simulation;

use crate::core::config::exp_config::ExperimentConfig;
use crate::core::config::sim_config::SimulationConfig;
use crate::core::logger::{Logger, StdoutLogger, TraceLogger};
use crate::simulation::CloudSimulation;

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

    fn on_simulation_finish(&mut self, _sim: Rc<RefCell<CloudSimulation>>) {
        // custom callback
    }
}

clone_trait_object!(SimulationCallbacks);

fn process_test_case(
    current_config: SimulationConfig,
    callbacks: &mut Box<dyn SimulationCallbacks>,
    trace_file: Option<String>,
) {
    let mut logger: Box<dyn Logger> = Box::new(StdoutLogger::new());
    if trace_file.is_some() {
        logger = Box::new(TraceLogger::new());
    }

    let sim = Simulation::new(123);
    let cloud_sim = rc!(refcell!(CloudSimulation::new_with_logger(
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
    callbacks.on_simulation_finish(cloud_sim.clone());

    if trace_file.is_some() {
        let save_result = cloud_sim
            .borrow()
            .logger()
            .borrow()
            .save_to_file(&trace_file.clone().unwrap());
        match save_result {
            Ok(_) => println!("Trace saved succesfully to file: {}", trace_file.unwrap()),
            Err(e) => println!("Error while saving trace: {e:?}"),
        }
    }
}

pub struct Experiment {
    pub callbacks: Box<dyn SimulationCallbacks>,
    pub config: ExperimentConfig,
    pub threads: usize,
    pub trace_file: Option<String>,
}

impl Experiment {
    pub fn new(
        callbacks: Box<dyn SimulationCallbacks>,
        config: ExperimentConfig,
        threads: usize,
        trace_file: Option<String>,
    ) -> Self
    where
        Self: Sized,
    {
        Self {
            callbacks,
            config,
            threads,
            trace_file,
        }
    }

    pub fn run(&mut self) {
        let pool = ThreadPool::new(self.threads);
        while let Some(current_config) = self.config.get() {
            let config_info = format!("{0:?}", self.config);
            let callbacks = self.callbacks.clone();
            let trace_file = self.trace_file.clone();
            pool.execute(move || {
                println!();
                println!();
                println!("==== New test case ====");
                println!("{}", config_info);
                println!("=======================");
                println!();
                println!();

                process_test_case(current_config.clone(), &mut callbacks.clone(), trace_file);
            });
        }
        pool.join();
    }
}
