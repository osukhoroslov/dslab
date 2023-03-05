//! CPU power model based on empirical evaluation of real physical host power consumption.

use crate::power::power_model::CPUPowerModel;

/// CPU power model based on empirical evaluation of real physical host power consumption.
/// The model is given list of 10 points representing a power consumption on thses levels
/// of CPU utilization. Thus, the 10% point is used if current CPU utilization is within 10 and 19 percent.
#[derive(Clone)]
pub struct EmpiricalPowerModel {
    #[allow(dead_code)]
    max_power: f64,
    utils: Vec<f64>,
}

impl EmpiricalPowerModel {
    /// Creates linear power model with specified parameters.
    ///
    /// * `max_power` - The maximum power consumption (at 100% utilization).
    /// * `utils` - Power consumptions for different CPU utilizations with 10% step.
    pub fn new(max_power: f64, utils: Vec<f64>) -> Self {
        if utils.len() != 11 {
            panic!("Incorrect utils size for EmpiricalPowerModel, should be 11")
        }
        Self { max_power, utils }
    }

    /// ETAS: Energy and thermal‐aware dynamic virtual machine consolidation in cloud data center with proactive hotspot mitigation
    ///
    /// April 2019Concurrency and Computation Practice and Experience 31(1):e5221
    /// DOI:10.1002/cpe.5221
    ///
    /// Intel Xeon X5675 CPU empirical evaluation
    pub fn xeon_x5675() -> Self {
        Self {
            max_power: 222.,
            utils: vec![0.26, 0.44, 0.49, 0.53, 0.57, 0.63, 0.68, 0.76, 0.85, 0.92, 1.],
        }
    }
}

impl CPUPowerModel for EmpiricalPowerModel {
    fn get_power(&self, _time: f64, utilization: f64) -> f64 {
        if utilization == 0. {
            return 0.;
        }

        *self.utils.get((utilization * 100.).floor() as usize / 10).unwrap() * self.max_power
    }
}
