//! Empirical CPU power model.

use crate::power::cpu::CpuPowerModel;

/// A power model based on measurements of actual power consumption at different utilization levels.
///
/// The model uses 11 measurements corresponding to power consumption in W at utilization levels from 0% to 100%
/// with step 10%, such as measurements reported by the [SPECpower benchmark](https://www.spec.org/power_ssj2008/results/).
///
/// The power consumption is computed using linear interpolation between the closest measurements.
#[derive(Clone)]
pub struct EmpiricalPowerModel {
    measurements: Vec<f64>,
}

impl EmpiricalPowerModel {
    /// Creates an empirical power model.
    ///
    /// * `measurements` - Power consumption measurements for utilization levels from 0% to 100% with 10% step.
    pub fn new(measurements: Vec<f64>) -> Self {
        assert_eq!(
            measurements.len(),
            11,
            "Incorrect measurements size for EmpiricalPowerModel, should be 11"
        );
        Self { measurements }
    }

    /// Empirical power model for IBM System x3550 M3 server with Intel Xeon X5675 CPU based on measurements
    /// from [SPECpower benchmark](http://www.spec.org/power_ssj2008/results/res2011q2/power_ssj2008-20110406-00368.html).
    pub fn system_x3550_m3_xeon_x5675() -> Self {
        Self {
            measurements: vec![58.4, 98., 109., 118., 128., 140., 153., 170., 189., 205., 222.],
        }
    }
}

impl CpuPowerModel for EmpiricalPowerModel {
    fn get_power(&self, utilization: f64) -> f64 {
        if utilization % 0.1 == 0. {
            self.measurements[(utilization * 10.) as usize]
        } else {
            let floor_idx = (utilization * 10.).floor();
            let floor_power = self.measurements[floor_idx as usize];
            let ceil_power = self.measurements[(utilization * 10.).ceil() as usize];
            floor_power + (ceil_power - floor_power) * (utilization - floor_idx / 10.) * 10.
        }
    }
}
