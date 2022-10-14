//! Power consumption models.

use dyn_clone::{clone_trait_object, DynClone};

/// Computes the host power consumption using the provided power model.
///
/// Optionally allows to assume no power consumption when the host is idle.
#[derive(Clone)]
pub struct HostPowerModel {
    power_model: Box<dyn PowerModel>,
    zero_idle_power: bool,
}

impl HostPowerModel {
    /// Creates host power model.
    ///
    /// * `power_fn` - Function for computing the host power consumption.
    pub fn new(power_fn: Box<dyn PowerModel>) -> Self {
        Self {
            power_model: power_fn,
            zero_idle_power: false,
        }
    }

    /// Modifies the model to assume no power consumption when the host is idle.
    pub fn with_zero_idle_power(mut self) -> Self {
        self.zero_idle_power = true;
        self
    }

    /// Returns the current power consumption of a physical host.
    pub fn get_power(&self, time: f64, cpu_util: f64) -> f64 {
        if cpu_util == 0. && self.zero_idle_power {
            return 0.;
        } else {
            self.power_model.get_power(time, cpu_util)
        }
    }
}

/// Model for computing power consumption of some component.
pub trait PowerModel: DynClone {
    /// Computes the current power consumption.
    ///
    /// * `time` - current simulation time.
    /// * `utilization` - current component utilization (0-1).
    fn get_power(&self, time: f64, utilization: f64) -> f64;
}

clone_trait_object!(PowerModel);

/// A power model with constant power consumption value.
#[derive(Clone)]
pub struct ConstantPowerModel {
    power: f64,
}

impl ConstantPowerModel {
    /// Creates constant power model with specified parameters.
    ///
    /// * `power` - Power consumption value.
    pub fn new(power: f64) -> Self {
        Self { power }
    }
}

impl PowerModel for ConstantPowerModel {
    fn get_power(&self, _time: f64, _utilization: f64) -> f64 {
        self.power
    }
}

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
        self.min_power + self.factor * utilization
    }
}
