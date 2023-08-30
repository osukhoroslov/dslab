//! Simulation configuration.

use serde::{Deserialize, Serialize};

use crate::extensions::dataset_type::VmDatasetType;

/// Auxiliary structure to parse SimulationConfig from file
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct SimulationConfigRaw {
    /// periodically send statistics from host to monitoring
    pub send_stats_period: Option<f64>,
    /// message trip time from any host to any direction
    pub message_delay: Option<f64>,
    /// when allocation request fails then wait for this duration
    pub allocation_retry_period: Option<f64>,
    /// vm initialization duration
    pub vm_start_duration: Option<f64>,
    /// vm deallocation duration
    pub vm_stop_duration: Option<f64>,
    /// pack VM by real resource consumption, not SLA
    pub allow_vm_overcommit: Option<bool>,
    /// currently used to define VM migration duration
    pub network_throughput: Option<u64>,
    /// length of simulation (for public datasets only)
    pub simulation_length: Option<f64>,
    /// duration beetween user access the simulation info
    pub step_duration: Option<f64>,
    /// VM becomes failed after this timeout is reached
    pub vm_allocation_timeout: Option<f64>,
    /// Dataset of virtual machines
    pub trace: Option<VmDatasetConfig>,
    /// cloud physical hosts
    pub hosts: Option<Vec<HostConfig>>,
    /// cloud schedulers
    pub schedulers: Option<Vec<SchedulerConfig>>,
}

/// Represents virtual machines dataset supported by this framework.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct VmDatasetConfig {
    /// dataset type, one of supported by dslab framework
    pub r#type: VmDatasetType,
    /// dataset file path where data is stored
    pub path: String,
}

impl std::fmt::Display for VmDatasetConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "vm dataset config, path = {}", self.path)
    }
}

/// Represents scheduler(s) configuration.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct SchedulerConfig {
    /// Scheduler name. Should be set if count = 1
    pub name: Option<String>,
    /// Scheduler name prefix. Full name is produced by appending instance number to the prefix.
    /// Should be set if count > 1
    pub name_prefix: Option<String>,
    /// VM placement algorithm for this scheduler
    pub algorithm: String,
    /// number of such schedulers
    pub count: u32,
}

/// Represents physical host(s) configuration.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct HostConfig {
    /// Host name. Should be set if count = 1
    pub name: Option<String>,
    /// Host name prefix. Full name is produced by appending instance number to the prefix.
    /// Should be set if count > 1
    pub name_prefix: Option<String>,
    /// host CPU capacity
    pub cpus: u32,
    /// host memory capacity
    pub memory: u64,
    /// number of such hosts
    pub count: Option<u32>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct SimulationConfig {
    /// periodically send statistics from host to monitoring
    pub send_stats_period: f64,
    /// message trip time from any host to any direction
    pub message_delay: f64,
    /// when allocation request fails then wait for this duration
    pub allocation_retry_period: f64,
    /// vm initialization duration
    pub vm_start_duration: f64,
    /// vm deallocation duration
    pub vm_stop_duration: f64,
    /// pack VM by real resource consumption, not SLA
    pub allow_vm_overcommit: bool,
    /// currently used to define VM migration duration
    pub network_throughput: u64,
    /// length of simulation (for public datasets only)
    pub simulation_length: f64,
    /// duration beetween user access the simulation info
    pub step_duration: f64,
    /// VM becomes failed after this timeout is reached
    pub vm_allocation_timeout: f64,
    /// Dataset of virtual machines
    pub trace: Option<VmDatasetConfig>,
    /// cloud physical hosts
    pub hosts: Vec<HostConfig>,
    /// cloud schedulers
    pub schedulers: Vec<SchedulerConfig>,
}

impl SimulationConfig {
    /// Returns total hosts count
    pub fn number_of_hosts(&self) -> u32 {
        let mut result = 0;
        for host in self.hosts.clone().into_iter() {
            result += host.count.unwrap_or(1);
        }
        result
    }

    /// Creates simulation config by reading parameter values from .yaml file (uses default values if some parameters are absent).
    pub fn from_file(file_name: &str) -> Self {
        let raw: SimulationConfigRaw = serde_yaml::from_str(
            &std::fs::read_to_string(file_name).unwrap_or_else(|_| panic!("Can't read file {}", file_name)),
        )
        .unwrap_or_else(|_| panic!("Can't parse YAML from file {}", file_name));

        Self {
            send_stats_period: raw.send_stats_period.unwrap_or(0.5),
            message_delay: raw.message_delay.unwrap_or(0.2),
            allocation_retry_period: raw.allocation_retry_period.unwrap_or(1.0),
            vm_start_duration: raw.vm_start_duration.unwrap_or(1.),
            vm_stop_duration: raw.vm_stop_duration.unwrap_or(0.5),
            allow_vm_overcommit: raw.allow_vm_overcommit.unwrap_or(false),
            network_throughput: raw.network_throughput.unwrap_or(1),
            simulation_length: raw.simulation_length.unwrap_or(0.),
            step_duration: raw.step_duration.unwrap_or(500.),
            vm_allocation_timeout: raw.vm_allocation_timeout.unwrap_or(50.),
            trace: raw.trace,
            hosts: raw.hosts.unwrap_or_default(),
            schedulers: raw.schedulers.unwrap_or_default(),
        }
    }
}
