//! Asymptotic CPU power model.

use std::f64::consts::E;

use crate::power::power_model::CPUPowerModel;

/// A power model based on non-linear interpolation between the minimum and maximum power consumption values.
/// Current power consumption is computed as P_curr = P_idle + (P_full - P_idle) / 2 * (1 + u - e ^ (-u / 0.3))
#[derive(Clone)]
pub struct AsymptoticPowerModel {
    #[allow(dead_code)]
    max_power: f64,
    min_power: f64,
    factor: f64,
}

impl AsymptoticPowerModel {
    /// Creates asymptotic power model with specified parameters.
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

impl CPUPowerModel for AsymptoticPowerModel {
    fn get_power(&self, utilization: f64) -> f64 {
        if utilization == 0. {
            return 0.;
        }
        self.min_power + self.factor * (1. + utilization - E.powf(-utilization * 10.)) / 2.
    }
}
