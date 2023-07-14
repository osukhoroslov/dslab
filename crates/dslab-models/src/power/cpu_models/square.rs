//! Square CPU power model.

use crate::power::cpu::CpuPowerModel;

/// A power model based on square interpolation between the minimum and maximum power consumption values.
#[derive(Clone)]
pub struct SquareCpuPowerModel {
    min_power: f64,
    #[allow(dead_code)]
    max_power: f64,
    factor: f64,
}

impl SquareCpuPowerModel {
    /// Creates a square power model.
    ///
    /// * `min_power` - The minimum power consumption in Watts (at 0% utilization).
    /// * `max_power` - The maximum power consumption in Watts (at 100% utilization).
    pub fn new(min_power: f64, max_power: f64) -> Self {
        Self {
            min_power,
            max_power,
            factor: max_power - min_power,
        }
    }
}

impl CpuPowerModel for SquareCpuPowerModel {
    fn get_power(&self, utilization: f64, _frequency: Option<f64>, _state: Option<usize>) -> f64 {
        self.min_power + self.factor * utilization.powf(2.)
    }
}
