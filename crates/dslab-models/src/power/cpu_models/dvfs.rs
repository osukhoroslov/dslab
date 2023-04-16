//! DVFS CPU power model.

use crate::power::cpu::CpuPowerModel;

/// A power model based on Dynamic Voltage  and Frequency Scaling (DVFS) techniques.
/// In addition to CPU utilization it`s current CPU frequency is taken into account.
///
/// https://www2.seas.gwu.edu/~howie/publications/CCGrid13.pdf
#[derive(Clone)]
pub struct DVFSCpuPowerModel {
    static_power: f64,
    max_power: f64,
    util_coef: f64,
    freq_coef: f64,
}

impl DVFSCpuPowerModel {
    /// Creates a linear power model.
    ///
    /// * `static_power` - The minimum power consumption in W (at 0% utilization).
    /// * `max_power` - The maximum power consumption in W (at 1000% utilization).
    pub fn new(static_power: f64, max_power: f64) -> Self {
        Self {
            static_power,
            max_power,
            util_coef: 0.2,
            freq_coef: 0.4,
        }
    }
}

impl CpuPowerModel for DVFSCpuPowerModel {
    /// Assume frequency always has the highest utilization
    fn get_power(&self, utilization: f64) -> f64 {
        self.static_power + (self.util_coef + self.freq_coef) * utilization * self.max_power
    }

    fn get_power_with_freq(&self, utilization: f64, frequency: f64) -> f64 {
        self.static_power + (self.util_coef + self.freq_coef * frequency) * utilization * self.max_power
    }
}
