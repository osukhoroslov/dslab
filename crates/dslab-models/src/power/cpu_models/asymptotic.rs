//! Asymptotic CPU power model.

use std::f64::consts::E;

use crate::power::cpu::CpuPowerModel;

/// A non-linear power consumption model from
/// [Kliazovich et al. e-STAB: Energy-Efficient Scheduling for Cloud Computing Applications with Traffic Load Balancing
/// (GreenCom 2013)](https://ieeexplore.ieee.org/abstract/document/6682042).
///
/// The power consumption is computed as `P(u) = P_min + 1/2 (P_max - P_min) * (1 + u - e^(-u / tau))`,
/// where `tau` is the utilization level at which the server attains asymptotic (close to linear) power consumption,
/// which is typically in the 0.5-0.8 range.
#[derive(Clone)]
pub struct AsymptoticCpuPowerModel {
    min_power: f64,
    #[allow(dead_code)]
    max_power: f64,
    tau: f64,
    factor: f64,
}

impl AsymptoticCpuPowerModel {
    /// Creates an asymptotic power model.
    ///
    /// * `min_power` - The minimum power consumption in Watts (at 0% utilization).
    /// * `max_power` - The maximum power consumption in Watts (at 100% utilization).
    /// * `tau` - The utilization level at which the server attains asymptotic power consumption.
    pub fn new(min_power: f64, max_power: f64, tau: f64) -> Self {
        Self {
            min_power,
            max_power,
            tau,
            factor: max_power - min_power,
        }
    }
}

impl CpuPowerModel for AsymptoticCpuPowerModel {
    fn get_power(&self, utilization: f64, _frequency: Option<f64>, _state: Option<usize>) -> f64 {
        self.min_power + self.factor * (1. + utilization - E.powf(-utilization / self.tau)) / 2.
    }
}
