//! Tools for launching experiments with multiple environment configurations

use std::cell::RefCell;
use std::rc::Rc;

use sugars::{rc, refcell};

use dslab_core::Simulation;

use crate::core::config::exp_config::ExperimentConfig;
use crate::simulation::CloudSimulation;

/// Callbacks on simulation events
pub trait SimulationCallbacks {
    fn on_simulation_start(&mut self, _sim: Rc<RefCell<CloudSimulation>>) {
        // custom callback
    }

    fn on_step(&mut self, _sim: Rc<RefCell<CloudSimulation>>) {
        // custom callback
    }

    fn on_simulation_finish(&mut self, _sim: Rc<RefCell<CloudSimulation>>) {
        // custom callback
    }
}

pub struct Experiment {
    pub callbacks: Box<dyn SimulationCallbacks>,
    pub config: ExperimentConfig,
}

impl Experiment {
    pub fn new(callbacks: Box<dyn SimulationCallbacks>, config: ExperimentConfig) -> Self
    where
        Self: Sized,
    {
        Self { callbacks, config }
    }

    pub fn run(&mut self) {
        loop {
            let sim = Simulation::new(123);
            let current_config = self.config.get();
            let cloud_sim = rc!(refcell!(CloudSimulation::new(sim, current_config.clone())));
            self.callbacks.on_simulation_finish(cloud_sim.clone());

            while cloud_sim.borrow_mut().current_time() < current_config.simulation_length {
                cloud_sim.borrow_mut().steps(1);
                self.callbacks.on_step(cloud_sim.clone());
            }
            self.callbacks.on_simulation_finish(cloud_sim.clone());

            if !self.config.has_next() {
                break;
            }
            self.config.next();
        }
    }
}
