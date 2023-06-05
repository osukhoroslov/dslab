//! Tools for launching experiments with multiple environment configurations

use std::ops::Deref;
use std::rc::Rc;

use sugars::rc;

use crate::core::config::SimulationConfig;
use crate::simulation::CloudSimulation;

/// Callback on test case finish, can be used to save or display single experiment run results
pub trait Experiment {
    fn new(sim: CloudSimulation) -> Self
    where
        Self: Sized;

    fn set_sim(&mut self, sim: CloudSimulation);

    fn get_sim(&self) -> Rc<CloudSimulation>;

    fn on_experiment_finish(&mut self);
}

pub struct DefaultExperiment {
    pub sim: CloudSimulation,
}

impl Experiment for DefaultExperiment {
    fn new(sim: CloudSimulation) -> Self
    where
        Self: Sized,
    {
        Self { sim }
    }

    fn get_sim(&self) -> Rc<CloudSimulation> {
        rc!(self.sim.clone())
    }

    fn set_sim(&mut self, sim: CloudSimulation) {
        self.sim = sim;
    }

    fn on_experiment_finish(&mut self) {
        // Custom callback
    }
}

pub fn run_experiments(mut experiment: Box<dyn Experiment>) {
    let sim = experiment.get_sim().deref().clone();
    let mut config: SimulationConfig = sim.sim_config().deref().clone();

    while config.can_increment() {
        let mut test_case = sim.clone();
        test_case.set_sim_config(config.clone());
        config.increment();

        test_case.step_for_duration(*config.simulation_length);

        experiment.set_sim(test_case);
        experiment.on_experiment_finish();
    }
}
