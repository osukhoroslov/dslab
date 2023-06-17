//! State-based CPU power model.

use std::collections::HashMap;

use crate::power::cpu::CpuPowerModel;

/// A power model that takes into account CPU power management states
/// by using a separate sub-model for each state.
///
/// Most modern processors have power management states such as P-states and C-states for Intel® processors.
/// P-states are states (voltage-frequency pairs) that are used when CPU is busy to reduce its power by slowing it down
/// and reducing its voltage. C-states are idle power saving states that shutdown the parts of CPU when it is unused.
///
/// This abstract model allows to specify an arbitrary set of CPU states and associate an arbitrary power model
/// with each of these states. When the model is invoked via `get_power()` method it obtains the current CPU state
/// from the `state` argument (resorting to default state if the state is unknown) and invokes the corresponding model.
#[derive(Clone)]
pub struct StateBasedCpuPowerModel {
    state_models: HashMap<String, Box<dyn CpuPowerModel>>,
    default_state: String,
}

impl StateBasedCpuPowerModel {
    /// Creates a state-based power model.
    ///
    /// * `state_models` - Map holding a power model for each modeled state.
    /// * `default_state` - Default state to use if the actual state is unknown.
    pub fn new<S>(state_models: HashMap<S, Box<dyn CpuPowerModel>>, default_state: S) -> Self
    where
        S: Into<String>,
    {
        let default_state = default_state.into();
        let state_models: HashMap<String, Box<dyn CpuPowerModel>> =
            state_models.into_iter().map(|(k, v)| (k.into(), v)).collect();
        assert!(
            state_models.contains_key(&default_state),
            "Missing model for default state {}",
            default_state
        );
        Self {
            state_models,
            default_state,
        }
    }
}

impl CpuPowerModel for StateBasedCpuPowerModel {
    fn get_power(&self, utilization: f64, frequency: Option<f64>, state: Option<String>) -> f64 {
        let state = if state.is_some() && self.state_models.contains_key(state.as_ref().unwrap()) {
            state.as_ref().unwrap()
        } else {
            &self.default_state
        };
        self.state_models
            .get(state)
            .unwrap()
            .get_power(utilization, frequency, None)
    }
}
