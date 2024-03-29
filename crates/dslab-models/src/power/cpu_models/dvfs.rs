//! DVFS-aware CPU power model.

use crate::power::cpu::CpuPowerModel;

/// A power model that takes into account the impact of Dynamic Voltage and Frequency Scaling (DVFS) techniques
/// adapted from [Xu et al. DUAL: Reliability-Aware Power Management in Data Centers (CCGrid 2013)](https://dl.acm.org/doi/abs/10.1109/CCGrid.2013.82).
///
/// In addition to CPU utilization it's current voltage and frequency are taken into account.
/// Because CPU frequencies are paired with voltages, the frequency is used to represent the voltage and frequency pair.
/// The current frequency is passed as the relative scaling of frequency between the minimum and maximum values.
///
/// The power consumption is computed as `P(util, freq) = static_power + util_coef * util + freq_coef * util * freq`.
/// When the CPU utilization is zero, the power consumed is constant no matter which DVFS state CPU is currently in.
/// When the CPU utilization is non-zero, the DVFS state affects the CPU dynamic power proportionally.
///
/// Note that the model coefficients depend on the workload and should be carefully chosen based on the empirical data.
/// Different workloads may have different power characteristics - for some applications power is sensitive to the
/// CPU frequency, while for others increasing the CPU frequency does not significantly increase its power consumption.
#[derive(Clone)]
pub struct DvfsAwareCpuPowerModel {
    static_power: f64,
    util_coef: f64,
    freq_coef: f64,
}

impl DvfsAwareCpuPowerModel {
    /// Creates a DVFS-aware power model.
    ///
    /// * `static_power` - The static power consumption in Watts (at 0% utilization).
    /// * `util_coef` - The coefficient for CPU utilization (part of dynamic power generated solely by CPU utilization).
    /// * `freq_coef` - The coefficient for CPU frequency (part of dynamic power generated by CPU frequency).
    pub fn new(static_power: f64, util_coef: f64, freq_coef: f64) -> Self {
        Self {
            static_power,
            util_coef,
            freq_coef,
        }
    }
}

impl CpuPowerModel for DvfsAwareCpuPowerModel {
    fn get_power(&self, utilization: f64, frequency: Option<f64>, _state: Option<usize>) -> f64 {
        // If the frequency is not available, assume 1.0 value
        let frequency = frequency.unwrap_or(1.);
        self.static_power + self.util_coef * utilization + self.freq_coef * utilization * frequency
    }
}
