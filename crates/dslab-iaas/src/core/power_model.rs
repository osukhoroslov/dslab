//! Physical host power consumption models.

use dyn_clone::{clone_trait_object, DynClone};

/// Power model is a function, which computes the power consumption of a physical host
/// based on its current CPU load and simulation time.
pub trait PowerModel: DynClone {
    /// Returns the current power consumption of a physical host.
    ///
    /// - `time` - current simulation time.
    /// - `cpu_load` - current host CPU load.
    fn get_power(&self, time: f64, cpu_load: f64) -> f64;
}

clone_trait_object!(PowerModel);

/// Simple linear power model.
///
/// Computes host power consumption (relative to fully loaded host) as `0.4 + cpu_load * 0.6`,
/// where `cpu_load` is the current host CPU load.
///
/// If CPU load is zero, then it is assumed that the host is powered off and its power consumption is zero.
#[derive(Clone)]
pub struct LinearPowerModel {
    host_power: f64,
    idle_power: f64,
}

impl LinearPowerModel {
    /// Default constructor.
    /// - `host_power` - host maximum power, when CPU is fully loaded.
    pub fn new(host_power: f64) -> Self {
        Self {
            idle_power: 0.4,
            host_power,
        }
    }

    pub fn new_with_idle_power(host_power: f64, idle_power: f64) -> Self {
        Self { idle_power, host_power }
    }
}

impl PowerModel for LinearPowerModel {
    fn get_power(&self, _time: f64, cpu_load: f64) -> f64 {
        if cpu_load == 0. {
            return 0.;
        }
        let factor = self.host_power - self.idle_power;
        self.idle_power + cpu_load * factor
    }
}
