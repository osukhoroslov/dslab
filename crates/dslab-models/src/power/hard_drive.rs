//! Hard drive power model trait.

use dyn_clone::{clone_trait_object, DynClone};

/// State of hard drive.
#[derive(Clone, Copy)]
pub enum HardDriveState {
    /// Disk stops spinning and hrad is moved off the disk
    Standby,
    /// Disk is being spinned but less powered.
    Idle,
    /// Disk performs some I/O actions.
    Active,
}

/// A model for estimating the power consumption of hard drive based on its utilization.
pub trait HardDrivePowerModel: DynClone {
    /// Returns hard drive power consumption in W.
    ///
    /// Hard drive state shold be passed as HardDriveState enum.
    fn get_power(&self, hdd_state: HardDriveState) -> f64;
}

clone_trait_object!(HardDrivePowerModel);
