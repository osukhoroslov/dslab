//! Power consumption models.

use dyn_clone::{clone_trait_object, DynClone};

/// Power model of a physical host.
pub trait HostPowerModel: DynClone {
    /// Returns the current power consumption of a physical host.
    ///
    /// * `time` - current simulation time.
    /// * `cpu_load` - current host CPU load.
    fn get_power(&self, time: f64, cpu_load: f64) -> f64;
}

clone_trait_object!(HostPowerModel);

/// A power model based on linear interpolation between the minimum and maximum power consumption values.
#[derive(Clone)]
pub struct LinearPowerModel {
    #[allow(dead_code)]
    max_power: f64,
    min_power: f64,
    factor: f64,
    zero_idle_power: bool,
}

impl LinearPowerModel {
    /// Creates linear power model with specified parameters.
    ///
    /// * `max_power` - The maximum power consumption (at 100% utilization).
    /// * `min_power` - The minimum power consumption (at 0% utilization).
    /// * `zero_idle_power` - Assume no power consumption when idle (powered off).
    pub fn new(max_power: f64, min_power: f64, zero_idle_power: bool) -> Self {
        Self {
            min_power,
            max_power,
            factor: max_power - min_power,
            zero_idle_power,
        }
    }
}

impl HostPowerModel for LinearPowerModel {
    fn get_power(&self, _time: f64, cpu_load: f64) -> f64 {
        if cpu_load == 0. && self.zero_idle_power {
            return 0.;
        }
        self.min_power + self.factor * cpu_load
    }
}

/// A power model with constant power consumption value.
#[derive(Clone)]
pub struct ConstantPowerModel {
    power: f64,
    zero_idle_power: bool,
}

impl ConstantPowerModel {
    /// Creates constant power model with specified parameters.
    ///
    /// * `power` - Power consumption value.
    /// * `zero_idle_power` - Assume no power consumption when idle (powered off).
    pub fn new(power: f64, zero_idle_power: bool) -> Self {
        Self { power, zero_idle_power }
    }
}

impl HostPowerModel for ConstantPowerModel {
    fn get_power(&self, _time: f64, cpu_load: f64) -> f64 {
        if cpu_load == 0. && self.zero_idle_power {
            return 0.;
        }
        self.power
    }
}
