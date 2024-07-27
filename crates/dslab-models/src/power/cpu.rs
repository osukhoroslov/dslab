//! CPU power model trait.

use dyn_clone::{clone_trait_object, DynClone};

/// A model for estimating the power consumption of CPU based on its utilization
/// and, optionally, current CPU frequency and state.
/// Can also be used to model the power consumption of a whole host based on CPU utilization.
pub trait CpuPowerModel: DynClone {
    /// Returns CPU power consumption in Watts.
    ///
    /// * CPU utilization is passed as a float in `[0, 1]` range (1 - maximum utilization).
    /// * (Optional) Relative CPU frequency is passed as a float in `[0, 1]` range,
    ///   where 0 corresponds to the minimum CPU frequency and 1 corresponds to the maximum CPU frequency.
    /// * (Optional) CPU power management state is passed as a numerical index in `[0, num states)` range.
    fn get_power(&self, utilization: f64, frequency: Option<f64>, state: Option<usize>) -> f64;
}

clone_trait_object!(CpuPowerModel);
