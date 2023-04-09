//! Hard drive linear power model.

use crate::power::hard_drive::{HardDrivePowerModel, HardDriveState};

/// A power model using a state wise power model when three states are distinguished.
/// Deng, Y. 2011. What is the future of disk drives, death or rebirth? ACM Comput. Surv. 43, 3, Article 23 (April 2011).
#[derive(Clone)]
pub struct StateWisePowerModel {
    power_idle: f64,
    power_standby: f64,
    power_active: f64,
}

impl StateWisePowerModel {
    /// Creates a StateWise power model.
    ///
    /// IBM 36Z15 is selected as default disk model
    pub fn new() -> Self {
        Self {
            power_idle: 10.2,
            power_standby: 2.5,
            power_active: 13.5,
        }
    }
}

impl HardDrivePowerModel for StateWisePowerModel {
    fn get_power(&self, hdd_state: HardDriveState) -> f64 {
        match hdd_state {
            HardDriveState::Standby => self.power_standby,
            HardDriveState::Idle => self.power_idle,
            HardDriveState::Active => self.power_active,
        }
    }
}

impl Default for StateWisePowerModel {
    fn default() -> Self {
        Self::new()
    }
}
