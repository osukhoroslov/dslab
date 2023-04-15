//! MSE CPU power model.

use crate::power::cpu::CpuPowerModel;

/// A non-linear power consumption model from
/// [Fan et al. Power Provisioning for a Warehouse-sized Computer (ISCA 2007)](https://dl.acm.org/doi/abs/10.1145/1273440.1250665).
///
/// The power consumption is computed as `P(u) = P_min + (P_max - P_min) * (2u - u^r)`,
/// where `r` is the calibration parameter that is chosen such as to minimize the mean squared error (MSE)
/// to the actual power measurements (1.4 in the original study).
#[derive(Clone)]
pub struct MseCpuPowerModel {
    #[allow(dead_code)]
    max_power: f64,
    min_power: f64,
    r_param: f64,
    factor: f64,
}

impl MseCpuPowerModel {
    /// Creates a MSE power model.
    ///
    /// * `max_power` - The maximum power consumption in W (at 100% utilization).
    /// * `min_power` - The minimum power consumption in W (at 0% utilization).
    /// * `r` - The calibration parameter set to minimize the MSE.
    pub fn new(max_power: f64, min_power: f64, r_param: f64) -> Self {
        Self {
            min_power,
            max_power,
            r_param,
            factor: max_power - min_power,
        }
    }
}

impl CpuPowerModel for MseCpuPowerModel {
    fn get_power(&self, utilization: f64) -> f64 {
        self.min_power + self.factor * (2. * utilization - utilization.powf(self.r_param))
    }
}
