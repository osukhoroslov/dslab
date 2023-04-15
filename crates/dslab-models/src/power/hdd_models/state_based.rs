//! State-based HDD power model.

use crate::power::hdd::{HddPowerModel, HddState};

/// A power model using different power consumption values for each HDD power state.
///
/// See [Deng Y. What is the future of disk drives, death or rebirth? (ACM CSUR, 2011)](https://dl.acm.org/doi/abs/10.1145/1922649.1922660).
#[derive(Clone)]
pub struct StateBasedHddPowerModel {
    power_active: f64,
    power_idle: f64,
    power_standby: f64,
}

impl StateBasedHddPowerModel {
    /// Creates a state-based power model.
    ///
    /// The power consumption values for IBM 36Z15 are used by default.
    pub fn new(power_active: f64, power_idle: f64, power_standby: f64) -> Self {
        Self {
            power_active,
            power_idle,
            power_standby,
        }
    }

    /// Creates a state-based power model for IBM 36Z15, a high-performance server disk drive,
    /// based on the values from [Deng Y. What is the future of disk drives, death or rebirth? (ACM CSUR, 2011)](https://dl.acm.org/doi/abs/10.1145/1922649.1922660).
    pub fn ibm_36z15() -> Self {
        Self {
            power_active: 13.5,
            power_idle: 10.2,
            power_standby: 2.5,
        }
    }
}

impl HddPowerModel for StateBasedHddPowerModel {
    fn get_power(&self, hdd_state: HddState) -> f64 {
        match hdd_state {
            HddState::Active => self.power_active,
            HddState::Idle => self.power_idle,
            HddState::Standby => self.power_standby,
        }
    }
}
