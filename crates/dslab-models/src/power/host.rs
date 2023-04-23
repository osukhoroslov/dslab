//! Host power model.

use crate::power::cpu::CpuPowerModel;
use crate::power::cpu_models::constant::ConstantCpuPowerModel;
use crate::power::hdd::{HddPowerModel, HddState};
use crate::power::memory::MemoryPowerModel;

/// Stores host state properties essential to compute the current host power consumption.
///
/// If both `memory_read_util` and `memory_write_util` are set, they are used instead of `memory_util`.
#[derive(Clone)]
pub struct HostState {
    /// CPU utilization from 0 to 1.
    pub cpu_util: Option<f64>,
    /// CPU relative frequency from 0 to 1.
    pub cpu_freq: Option<f64>,
    /// CPU state. P-state or C-state.
    pub cpu_state: Option<String>,
    /// Memory utilization from 0 to 1.
    pub memory_util: Option<f64>,
    /// Memory read utilization from 0 to 1.
    pub memory_read_util: Option<f64>,
    /// Memory read utilization from 0 to 1.
    pub memory_write_util: Option<f64>,
    /// Hard disk drive state.
    pub hdd_state: Option<HddState>,
}

impl HostState {
    /// Creates HostState with specified properties.
    pub fn new(
        cpu_util: Option<f64>,
        cpu_freq: Option<f64>,
        cpu_state: Option<String>,
        memory_util: Option<f64>,
        memory_read_util: Option<f64>,
        memory_write_util: Option<f64>,
        hdd_state: Option<HddState>,
    ) -> Self {
        Self {
            cpu_util,
            cpu_freq,
            cpu_state,
            memory_util,
            memory_read_util,
            memory_write_util,
            hdd_state,
        }
    }

    /// Shortcut for building HostState from CPU utilization only.
    pub fn cpu(cpu_util: f64) -> Self {
        Self {
            cpu_util: Some(cpu_util),
            cpu_freq: None,
            cpu_state: None,
            memory_util: None,
            memory_read_util: None,
            memory_write_util: None,
            hdd_state: None,
        }
    }

    /// Shortcut for building HostState from memory utilization only.
    pub fn memory(memory_util: f64) -> Self {
        Self {
            cpu_util: None,
            cpu_freq: None,
            cpu_state: None,
            memory_util: Some(memory_util),
            memory_read_util: None,
            memory_write_util: None,
            hdd_state: None,
        }
    }

    /// Shortcut for building HostState from HDD state only.
    pub fn hdd(hdd_state: HddState) -> Self {
        Self {
            cpu_util: None,
            cpu_freq: None,
            cpu_state: None,
            memory_util: None,
            memory_read_util: None,
            memory_write_util: None,
            hdd_state: Some(hdd_state),
        }
    }
}

/// A model for estimating the power consumption of a physical host.
///
/// The host power consumption is modeled using the following parts:
/// - CPU power consumption estimated using the provided CPU power model
/// - memory power consumption estimated using the provided memory power model
/// - hard disk drive power consumption estimated using the provided HDD power model
/// - consumption of other host components modeled as a fixed value
#[derive(Clone)]
pub struct HostPowerModel {
    cpu_power_model: Option<Box<dyn CpuPowerModel>>,
    memory_power_model: Option<Box<dyn MemoryPowerModel>>,
    hard_drive_power_model: Option<Box<dyn HddPowerModel>>,
    other_power: f64,
}

impl HostPowerModel {
    /// Creates the host power model.
    pub fn new(
        cpu_power_model: Box<dyn CpuPowerModel>,
        memory_power_model: Box<dyn MemoryPowerModel>,
        hard_drive_power_model: Box<dyn HddPowerModel>,
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

    /// Creates the host power model using only the HDD power consumption part.
    pub fn hdd_only(hard_drive_power_model: Box<dyn HddPowerModel>) -> Self {
        Self {
            memory_power_model: None,
            cpu_power_model: None,
            hard_drive_power_model: Some(hard_drive_power_model),
            other_power: 0.,
        }
    }

    /// Returns the power consumption of a host in W for a given host state.
    pub fn get_power(&self, host_state: HostState) -> f64 {
        let mut result = 0.;
        if let Some(model) = &self.cpu_power_model {
            if let (Some(cpu_util), Some(cpu_freq)) = (host_state.cpu_util, host_state.cpu_freq) {
                result += model.get_power_with_freq(cpu_util, cpu_freq);
            } else if let (Some(cpu_state), Some(cpu_util)) = (host_state.cpu_state, host_state.cpu_util) {
                result += model.get_power_with_state(cpu_util, cpu_state);
            } else if let Some(cpu_util) = host_state.cpu_util {
                result += model.get_power(cpu_util);
            }
        }
        if let Some(model) = &self.memory_power_model {
            if let (Some(memory_read_util), Some(memory_write_util)) =
                (host_state.memory_read_util, host_state.memory_write_util)
            {
                result += model.get_power_advanced(memory_read_util, memory_write_util);
            } else if let Some(memory_util) = host_state.memory_util {
                result += model.get_power_simple(memory_util);
            }
        }
        if let Some(model) = &self.hard_drive_power_model {
            if let Some(hdd_state) = host_state.hdd_state {
                result += model.get_power(hdd_state);
            }
        }
        result += self.other_power;
        result
    }
}

impl Default for HostPowerModel {
    fn default() -> Self {
        Self::cpu_only(Box::new(ConstantCpuPowerModel::new(0.)))
    }
}
