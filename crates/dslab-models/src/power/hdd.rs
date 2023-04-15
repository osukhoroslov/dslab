//! Hard disk drive power model trait.

use dyn_clone::{clone_trait_object, DynClone};

/// Power states of hard disk drive.
///
/// See [Deng Y. What is the future of disk drives, death or rebirth? (ACM CSUR, 2011)](https://dl.acm.org/doi/abs/10.1145/1922649.1922660).
#[derive(Clone, Copy)]
pub enum HddState {
    /// The disk spins at full speed serving I/O requests.
    Active,
    /// The disk is spinning but does not service I/O requests
    /// (the electronics may be partially unpowered, and the heads may be parked or unloaded).
    Idle,
    /// The disk is spun down to reduce its power consumption
    /// (the disk stops spinning and the head is moved off the disk).
    Standby,
}

/// A model for estimating the power consumption of hard disk drive (HDD) based on its state.
pub trait HddPowerModel: DynClone {
    /// Returns the disk power consumption in W.
    fn get_power(&self, state: HddState) -> f64;
}

clone_trait_object!(HddPowerModel);
