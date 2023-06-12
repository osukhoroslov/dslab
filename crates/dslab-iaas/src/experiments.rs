//! Tools for launching experiments with multiple environment configurations

use dslab_core::Simulation;

use crate::core::config::SimulationConfig;
use crate::simulation::CloudSimulation;

/// Callback on test case finish, can be used to save or display single experiment run results
pub trait OnTestCaseFinishedCallback {
    fn on_experiment_finish(&self, _sim: &mut CloudSimulation)
    {
        // custom callback
    }
}

pub struct Experiment {
    pub final_callback: Box<dyn OnTestCaseFinishedCallback>,
    pub config: SimulationConfig,
}

impl Experiment {
    pub fn new(final_callback: Box<dyn OnTestCaseFinishedCallback>, config: SimulationConfig) -> Self
    where
        Self: Sized,
    {
        Self { final_callback, config }
    }

    pub fn start(&mut self) {
        while self.config.can_increment() {
            let sim = Simulation::new(123);
            let mut cloud_sim = CloudSimulation::new(sim, self.config.clone().data);
            cloud_sim.step_for_duration(*self.config.data.simulation_length);
            self.final_callback.on_experiment_finish(&mut cloud_sim);
            self.config.increment();
        }
    }
}
