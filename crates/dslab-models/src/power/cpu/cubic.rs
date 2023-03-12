//! CPU power model based on current host CPU utilization. The power consumption is
// considered in proportion to cube of current CPU load.

use crate::power::power_model::CPUPowerModel;

/// A power model based on cubic interpolation between the minimum and maximum power consumption values.
#[derive(Clone)]
pub struct CubicPowerModel {
    #[allow(dead_code)]
    max_power: f64,
    min_power: f64,
    factor: f64,
}

impl CubicPowerModel {
    /// Creates cubic power model with specified parameters.
    ///
    /// * `max_power` - The maximum power consumption (at 100% utilization).
    /// * `min_power` - The minimum power consumption, or idle (at 0% utilization).
    pub fn new(max_power: f64, min_power: f64) -> Self {
        Self {
            min_power,
            max_power,
            factor: max_power - min_power,
        }
    }
}

impl CPUPowerModel for CubicPowerModel {
    fn get_power(&self, utilization: f64) -> f64 {
        if utilization == 0. {
            return 0.;
        }
        self.min_power + self.factor * utilization.powf(3.)
    }
}
