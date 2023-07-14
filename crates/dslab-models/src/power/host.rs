//! Host power model.

use crate::power::cpu::CpuPowerModel;
use crate::power::hdd::{HddPowerModel, HddState};
use crate::power::memory::MemoryPowerModel;

/// Stores host state properties essential to compute the current host power consumption.
///
/// By default all properties are empty (None).
/// If both `memory_read_util` and `memory_write_util` are set, they are used instead of `memory_util`.
#[derive(Default, Clone, Copy)]
pub struct HostState {
    /// CPU utilization from 0 to 1.
    pub cpu_util: Option<f64>,
    /// Relative scaling of CPU frequency from 0 to 1,
    /// where 0 corresponds to the minimum CPU frequency
    /// and 1 corresponds to the maximum CPU frequency.
    pub cpu_freq: Option<f64>,
    /// CPU power management state, specified as an index in `[0, num states)` range.
    pub cpu_state: Option<usize>,
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
    /// Creates HostState with specified CPU utilization.
    pub fn cpu_util(cpu_util: f64) -> Self {
        Self {
            cpu_util: Some(cpu_util),
            ..Default::default()
        }
    }

    /// Creates HostState with specified CPU utilization and frequency.
    pub fn cpu_util_freq(cpu_util: f64, cpu_freq: f64) -> Self {
        Self {
            cpu_util: Some(cpu_util),
            cpu_freq: Some(cpu_freq),
            ..Default::default()
        }
    }

    /// Creates HostState with specified CPU utilization and state.
    pub fn cpu_util_state(cpu_util: f64, cpu_state: usize) -> Self {
        Self {
            cpu_util: Some(cpu_util),
            cpu_state: Some(cpu_state),
            ..Default::default()
        }
    }
}

/// A model for estimating the power consumption of a physical host.
///
/// The host power consumption is modeled using the following parts:
/// - CPU power consumption estimated using the provided CPU power model and idle power value
/// - memory power consumption estimated using the provided memory power model
/// - hard disk drive power consumption estimated using the provided HDD power model
/// - consumption of other host components modeled as a fixed value
#[derive(Clone, Default)]
pub struct HostPowerModel {
    cpu_power_model: Option<Box<dyn CpuPowerModel>>,
    cpu_idle_power: Option<f64>,
    memory_power_model: Option<Box<dyn MemoryPowerModel>>,
    hard_drive_power_model: Option<Box<dyn HddPowerModel>>,
    other_power: f64,
}

impl HostPowerModel {
    /// Returns the power consumption of a host in Watts for a given host state.
    pub fn get_power(&self, host_state: HostState) -> f64 {
        let mut result = 0.;
        let cpu_util = host_state.cpu_util.unwrap_or(0.);
        if cpu_util == 0. && self.cpu_idle_power.is_some() {
            result += self.cpu_idle_power.unwrap();
        } else if let Some(model) = &self.cpu_power_model {
            result += model.get_power(cpu_util, host_state.cpu_freq, host_state.cpu_state);
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

/// Helper for building the host power model.
#[derive(Default)]
pub struct HostPowerModelBuilder {
    cpu_power_model: Option<Box<dyn CpuPowerModel>>,
    cpu_idle_power: Option<f64>,
    memory_power_model: Option<Box<dyn MemoryPowerModel>>,
    hard_drive_power_model: Option<Box<dyn HddPowerModel>>,
    other_power: f64,
}

impl HostPowerModelBuilder {
    /// Creates the builder with default settings
    /// (all inner power models are None, other power consumption is 0).
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the CPU power model.
    pub fn cpu(mut self, model: Box<dyn CpuPowerModel>) -> Self {
        self.cpu_power_model = Some(model);
        self
    }

    /// Sets the CPU power consumption in idle state (0% utilization).
    pub fn cpu_idle(mut self, value: f64) -> Self {
        self.cpu_idle_power = Some(value);
        self
    }

    /// Sets the memory power model.
    pub fn memory(mut self, model: Box<dyn MemoryPowerModel>) -> Self {
        self.memory_power_model = Some(model);
        self
    }

    /// Sets the HDD power model.
    pub fn hard_drive(mut self, model: Box<dyn HddPowerModel>) -> Self {
        self.hard_drive_power_model = Some(model);
        self
    }

    /// Sets the power consumption of other host components.
    pub fn other(mut self, value: f64) -> Self {
        self.other_power = value;
        self
    }

    /// Builds the host power model.
    pub fn build(self) -> HostPowerModel {
        HostPowerModel {
            cpu_power_model: self.cpu_power_model,
            cpu_idle_power: self.cpu_idle_power,
            memory_power_model: self.memory_power_model,
            hard_drive_power_model: self.hard_drive_power_model,
            other_power: self.other_power,
        }
    }
}
