//! State-based CPU power model.

use crate::power::cpu::CpuPowerModel;

/// A power model that takes into account CPU power management states
/// by using a separate sub-model for each state.
///
/// Most modern processors have power management states such as P-states and C-states for Intel® processors.
/// P-states are states (voltage-frequency pairs) that are used when CPU is busy to reduce its power by slowing it down
/// and reducing its voltage. C-states are idle power saving states that shutdown the parts of CPU when it is unused.
///
/// This abstract model allows to specify an arbitrary set of CPU states and associate an arbitrary power model
/// with each of these states. When the model is invoked via `get_power()` method, it obtains the current CPU state
/// (passed as an index in `[0, num states)` range) from the `state` argument and invokes the corresponding model.
#[derive(Clone)]
pub struct StateBasedCpuPowerModel {
    state_models: Vec<Box<dyn CpuPowerModel>>,
}

impl StateBasedCpuPowerModel {
    /// Creates a state-based power model.
    ///
    /// * `state_models` - Vector holding a power model for each modeled state (state is referred by its index).
    pub fn new(state_models: Vec<Box<dyn CpuPowerModel>>) -> Self {
        Self { state_models }
    }
}

impl CpuPowerModel for StateBasedCpuPowerModel {
    fn get_power(&self, utilization: f64, frequency: Option<f64>, state: Option<usize>) -> f64 {
        self.state_models[state.expect("CPU state in unavailable")].get_power(utilization, frequency, None)
    }
}
