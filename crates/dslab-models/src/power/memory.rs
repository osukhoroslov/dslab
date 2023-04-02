//! Memory power model trait.

use dyn_clone::{clone_trait_object, DynClone};

/// A model for estimating the power consumption of memory based on its utilization.
pub trait MemoryPowerModel: DynClone {
    /// Returns memory power consumption in W.
    ///
    /// Memory utilization should be passed as a float in 0.0-1.0 range.
    fn get_power(&self, utilization: f64) -> f64;

    /// Returns memory power consumption in W.
    ///
    /// Memory read and write utilization should be passed as a float in 0.0-1.0 range.
    /// Has more priority than single utilization.
    fn get_power_adv(&self, read_util: f64, write_util: f64) -> f64;
}

clone_trait_object!(MemoryPowerModel);
