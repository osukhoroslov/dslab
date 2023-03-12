//! MSE CPU power model.

use crate::power::power_model::CPUPowerModel;

/// A power model based on non-linear interpolation between the minimum and maximum power consumption values.
/// Current power consumption is computed as P_curr = P_idle + (P_full - P_idle) * (2 * u - u ^ 1.4)
///
/// Priya, B., Pilli, E. S., & Joshi, R. C. (2013).
/// A survey on energy and power consumption models for Greener Cloud.
/// 2013 3rd IEEE International Advance Computing Conference (IACC).
#[derive(Clone)]
pub struct MSEPowerModel {
    #[allow(dead_code)]
    max_power: f64,
    min_power: f64,
    factor: f64,
}

impl MSEPowerModel {
    /// Creates linear power model with specified parameters.
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

impl CPUPowerModel for MSEPowerModel {
    fn get_power(&self, utilization: f64) -> f64 {
        if utilization == 0. {
            return 0.;
        }
        self.min_power + self.factor * (2. * utilization - utilization.powf(1.4))
    }
}
