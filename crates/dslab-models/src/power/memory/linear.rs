//! Memory power model based on virtual machines current memory consumption.

use crate::power::power_model::MemoryPowerModel;

/// A power model based on virtual machines current memory consumption.
#[derive(Clone)]
pub struct LinearPowerModel {
    #[allow(dead_code)]
    max_power: f64,
}

impl LinearPowerModel {
    /// Creates linear power model with specified parameters.
    ///
    /// * `max_power` - The maximum power consumption (at 100% utilization).
    pub fn new(max_power: f64) -> Self {
        Self { max_power }
    }
}

impl MemoryPowerModel for LinearPowerModel {
    fn get_power(&self, _time: f64, utilization: f64) -> f64 {
        if utilization == 0. {
            return 0.;
        }

        self.max_power * utilization
    }
}
