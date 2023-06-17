//! CPU power model trait.

use dyn_clone::{clone_trait_object, DynClone};

/// A model for estimating the power consumption of CPU based on its utilization
/// and, optionally, current CPU frequency and state.
/// Can also be used to model the power consumption of a whole host based on CPU utilization.
pub trait CpuPowerModel: DynClone {
    /// Returns CPU power consumption in Watts.
    ///
    /// CPU utilization should be passed as a float in 0-1 range (1 - maximum utilization).
    /// (Optional) Relative CPU frequency should be passed as a float in 0-1 range,
    /// where 0 corresponds to the minimum CPU frequency and 1 corresponds to the maximum CPU frequency.
    /// (Optional) CPU state should be passed as 'Px' or 'Cx'.
    fn get_power(&self, utilization: f64, frequency: Option<f64>, state: Option<String>) -> f64;
}

clone_trait_object!(CpuPowerModel);
