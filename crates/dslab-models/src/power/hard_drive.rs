//! Hard drive power model trait.

use dyn_clone::{clone_trait_object, DynClone};

/// A model for estimating the power consumption of hard drive based on its utilization.
pub trait HardDrivePowerModel: DynClone {
    /// Returns hard drive power consumption in W.
    ///
    /// Hard drive I/O utilization should be passed as a float in 0.0-1.0 range.
    fn get_power(&self, utilization: f64) -> f64;
}

clone_trait_object!(HardDrivePowerModel);
