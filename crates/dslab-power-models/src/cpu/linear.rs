use crate::power_model::PowerModel;

/// A power model based on linear interpolation between the minimum and maximum power consumption values.
#[derive(Clone)]
pub struct LinearPowerModel {
    #[allow(dead_code)]
    max_power: f64,
    min_power: f64,
    factor: f64,
}

impl LinearPowerModel {
    /// Creates linear power model with specified parameters.
    ///
    /// * `max_power` - The maximum power consumption (at 100% utilization).
    /// * `min_power` - The minimum power consumption (at 0% utilization).
    pub fn new(max_power: f64, min_power: f64) -> Self {
        Self {
            min_power,
            max_power,
            factor: max_power - min_power,
        }
    }
}

impl PowerModel for LinearPowerModel {
    fn get_power(&self, _time: f64, utilization: f64) -> f64 {
        if utilization == 0. {
            return 0.;
        }
        self.min_power + self.factor * utilization
    }
}
