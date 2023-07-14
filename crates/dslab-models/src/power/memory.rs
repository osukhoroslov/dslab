//! Memory power model trait.

use dyn_clone::{clone_trait_object, DynClone};

/// A model for estimating the power consumption of memory based on its utilization.
pub trait MemoryPowerModel: DynClone {
    /// Returns memory power consumption in Watts based on a single utilization value.
    ///
    /// Memory utilization should be passed as a float in 0.0-1.0 range.
    fn get_power_simple(&self, utilization: f64) -> f64;

    /// Returns memory power consumption in Watts based on separate read and write utilization values.
    ///
    /// Memory read and write utilization should be passed as floats in 0.0-1.0 range.
    /// Has more priority than [`get_power_simple()`](Self::get_power_simple).
    fn get_power_advanced(&self, read_util: f64, write_util: f64) -> f64;
}

clone_trait_object!(MemoryPowerModel);
