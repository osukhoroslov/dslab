//! Tools for launching experiments with multiple environment configurations

use std::cell::RefCell;
use std::rc::Rc;

use dyn_clone::{clone_trait_object, DynClone};
use sugars::{rc, refcell};
use threadpool::ThreadPool;

use dslab_core::Simulation;

use crate::core::config::exp_config::ExperimentConfig;
use crate::core::config::sim_config::SimulationConfig;
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

fn process_test_case(current_config: SimulationConfig, callbacks: &mut Box<dyn SimulationCallbacks>) {
    let sim = Simulation::new(123);
    let cloud_sim = rc!(refcell!(CloudSimulation::new(sim, current_config.clone())));
    callbacks.on_simulation_start(cloud_sim.clone());

    while cloud_sim.borrow_mut().current_time() < current_config.simulation_length {
        cloud_sim.borrow_mut().steps(1);
        let proceed = callbacks.on_step(cloud_sim.clone());
        if !proceed {
            break;
        }
    }
    callbacks.on_simulation_finish(cloud_sim);
}

pub struct Experiment {
    pub callbacks: Box<dyn SimulationCallbacks>,
    pub config: ExperimentConfig,
    pub threads: Option<usize>,
}

impl Experiment {
    pub fn new(callbacks: Box<dyn SimulationCallbacks>, config: ExperimentConfig, threads: Option<usize>) -> Self
    where
        Self: Sized,
    {
        Self {
            callbacks,
            config,
            threads,
        }
    }

    pub fn run(&mut self) {
        let pool = ThreadPool::new(self.threads.unwrap_or(1));

        while let Some(current_config) = self.config.get() {
            println!();
            println!();
            println!("==== New test case ====");
            println!("{0:?}", self.config);
            println!("=======================");
            println!();
            println!();

            let callbacks = self.callbacks.clone();
            if self.threads.is_some() {
                pool.execute(move || {
                    process_test_case(current_config.clone(), &mut callbacks.clone());
                });
            } else {
                process_test_case(current_config.clone(), &mut callbacks.clone());
            }
        }
        pool.join();
    }
}
