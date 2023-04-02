//! Host power model.

use crate::power::cpu::CpuPowerModel;
use crate::power::cpu_models::constant::ConstantPowerModel;
use crate::power::hard_drive::HardDrivePowerModel;
use crate::power::memory::MemoryPowerModel;

/// Current host state properties essential to compute current host power consumption.
///
/// `cpu_util` - CPU utilization from 0. to 1.
/// `memory_util` - memory utilization from 0. to 1.
/// `memory_read_util` - memory read footprint utilization from 0. to 1.
/// `memory_write_util` - memory read footprint utilization from 0. to 1.
/// `hdd_util` - hard drive utilization from 0. to 1.
///
/// If both `memory_read_util` and `memory_write_util` are both set, they are used instead of `memory_util`.
#[derive(Clone)]
pub struct HostState {
    /// `cpu_util` - CPU utilization from 0. to 1.
    pub cpu_util: Option<f64>,
    /// `memory_util` - memory utilization from 0. to 1.
    pub memory_util: Option<f64>,
    /// `memory_read_util` - memory read footprint utilization from 0. to 1.
    pub memory_read_util: Option<f64>,
    /// `memory_write_util` - memory read footprint utilization from 0. to 1.
    pub memory_write_util: Option<f64>,
    /// `hdd_util` - hard drive utilization from 0. to 1.
    pub hdd_util: Option<f64>,
}

impl HostState {
    /// Shortcut for building HostState from CPU utilization only.
    pub fn cpu(cpu_util: f64) -> Self {
        Self {
            cpu_util: Some(cpu_util),
            memory_util: None,
            memory_read_util: None,
            memory_write_util: None,
            hdd_util: None,
        }
    }

    /// Shortcut for building HostState from memory utilization only.
    pub fn memory(memory_util: f64) -> Self {
        Self {
            cpu_util: None,
            memory_util: Some(memory_util),
            memory_read_util: None,
            memory_write_util: None,
            hdd_util: None,
        }
    }

    /// Shortcut for building HostState from HDD utilization only.
    pub fn hard_drive(hdd_util: f64) -> Self {
        Self {
            cpu_util: None,
            memory_util: None,
            memory_read_util: None,
            memory_write_util: None,
            hdd_util: Some(hdd_util),
        }
    }
}

/// A model for estimating the power consumption of a physical host.
///
/// The host power consumption is modeled as two parts:
/// - CPU consumption estimated using the provided CPU power model
/// - consumption of other host components modeled as a fixed value
#[derive(Clone)]
pub struct HostPowerModel {
    cpu_power_model: Option<Box<dyn CpuPowerModel>>,
    memory_power_model: Option<Box<dyn MemoryPowerModel>>,
    hard_drive_power_model: Option<Box<dyn HardDrivePowerModel>>,
    other_power: f64,
}

impl HostPowerModel {
    /// Creates the host power model using the provided CPU power model and power usage of other components.
    pub fn new(
        cpu_power_model: Box<dyn CpuPowerModel>,
        memory_power_model: Box<dyn MemoryPowerModel>,
        hard_drive_power_model: Box<dyn HardDrivePowerModel>,
        other_power: f64,
    ) -> Self {
        Self {
            cpu_power_model: Some(cpu_power_model),
            memory_power_model: Some(memory_power_model),
            hard_drive_power_model: Some(hard_drive_power_model),
            other_power,
        }
    }

    /// Creates the host power model using only the CPU power consumption part.
    pub fn cpu_only(cpu_power_model: Box<dyn CpuPowerModel>) -> Self {
        Self {
            cpu_power_model: Some(cpu_power_model),
            memory_power_model: None,
            hard_drive_power_model: None,
            other_power: 0.,
        }
    }

    /// Creates the host power model using only the memory power consumption part.
    pub fn memory_only(memory_power_model: Box<dyn MemoryPowerModel>) -> Self {
        Self {
            memory_power_model: Some(memory_power_model),
            cpu_power_model: None,
            hard_drive_power_model: None,
            other_power: 0.,
        }
    }

    /// Creates the host power model using only the hard drive power consumption part.
    pub fn hdd_only(hard_drive_power_model: Box<dyn HardDrivePowerModel>) -> Self {
        Self {
            memory_power_model: None,
            cpu_power_model: None,
            hard_drive_power_model: Some(hard_drive_power_model),
            other_power: 0.,
        }
    }

    /// Returns the power consumption of a host (in W) based on CPU utilization.
    ///
    /// CPU utilization should be passed as a float in 0.0-1.0 range.
    pub fn get_power(&self, host_state: HostState) -> f64 {
        let mut result = 0.;
        if let Some(model) = &self.cpu_power_model {
            if let Some(cpu_util) = &host_state.cpu_util {
                result += model.get_power(*cpu_util);
            }
        }
        if let Some(model) = &self.memory_power_model {
            let memory_read_util = &host_state.memory_read_util;
            let memory_write_util = &host_state.memory_write_util;
            let memory_util = &host_state.memory_util;

            if memory_read_util.is_some() && memory_write_util.is_some() {
                result += model.get_power_adv(memory_read_util.unwrap(), memory_write_util.unwrap());
            } else if memory_util.is_some() {
                result += model.get_power(memory_util.unwrap());
            }
        }
        if let Some(model) = &self.hard_drive_power_model {
            if let Some(hdd_util) = &host_state.hdd_util {
                result += model.get_power(*hdd_util);
            }
        }
        result += self.other_power;
        result
    }
}

impl Default for HostPowerModel {
    fn default() -> Self {
        Self::cpu_only(Box::new(ConstantPowerModel::new(0.)))
    }
}
