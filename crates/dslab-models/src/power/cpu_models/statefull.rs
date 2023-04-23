//! P- and C- states CPU power model.

use std::collections::HashMap;

use crate::power::cpu::CpuPowerModel;

/// A power model based on CPU C- and P- states.
///
/// http://www.ilinuxkernel.com/files/CPU.Power/pwr_mgmt_states_r0.pdf
/// https://portal.nutanix.com/page/documents/kbs/details?targetId=kA00e000000CrLqCAK
///
/// Every CPU has several P-states which are set when CPU is busy by execution some instructions and
/// C-states or energy-save modes, when CPU is currently idle.
/// For every CPU state personal power model can be set by user.
#[derive(Clone)]
pub struct StatefullCpuPowerModel {
    cpu_models: HashMap<String, Box<dyn CpuPowerModel>>,
}

impl StatefullCpuPowerModel {
    /// Creates an statefull power model.
    ///
    /// * `default_model` - CPU power model for every CPU state not specifyed directly in add_cpu_model method.
    pub fn new(default_model: Box<dyn CpuPowerModel>) -> Self {
        let mut models = HashMap::<String, Box<dyn CpuPowerModel>>::new();
        models.insert("default".to_string(), default_model);

        Self { cpu_models: models }
    }

    /// Specify CPU power model from this crate for some CPU state.
    ///
    /// * `state` - CPU state where `model` will be used to calculate current power usage. Should be like Cx or Px
    /// * `model` - some CPU power model from this crate
    pub fn add_cpu_model(&mut self, state: String, model: Box<dyn CpuPowerModel>) {
        if state.len() != 2
            || (!state.starts_with('C') && !state.starts_with('P') || !state.chars().nth(1).unwrap().is_ascii_digit())
        {
            panic!("Incorrect CPU state");
        }

        self.cpu_models.insert(state, model);
    }
}

impl CpuPowerModel for StatefullCpuPowerModel {
    fn get_power(&self, _utilization: f64) -> f64 {
        0.
    }

    fn get_power_with_state(&self, utilization: f64, state: String) -> f64 {
        if !self.cpu_models.contains_key(&state) {
            return self.cpu_models.get("default").unwrap().get_power(utilization);
        }
        self.cpu_models.get(&state).unwrap().get_power(utilization)
    }
}
