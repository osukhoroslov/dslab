//! Simulation configuration.

use serde::{Deserialize, Serialize};

use crate::extensions::dataset_type::VmDatasetType;

/// Holds raw simulation config parsed from YAML file.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
struct RawSimulationConfig {
    pub send_stats_period: Option<f64>,
    pub message_delay: Option<f64>,
    pub allocation_retry_period: Option<f64>,
    pub vm_start_duration: Option<f64>,
    pub vm_stop_duration: Option<f64>,
    pub allow_vm_overcommit: Option<bool>,
    pub network_throughput: Option<u64>,
    pub simulation_length: Option<f64>,
    pub step_duration: Option<f64>,
    pub vm_allocation_timeout: Option<f64>,
    pub trace: Option<VmDatasetConfig>,
    pub hosts: Option<Vec<HostConfig>>,
    pub schedulers: Option<Vec<SchedulerConfig>>,
}

/// Holds information about the used VM trace dataset.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct VmDatasetConfig {
    /// Dataset type.
    pub r#type: VmDatasetType,
    /// Dataset path.
    pub path: String,
}

impl std::fmt::Display for VmDatasetConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "vm dataset config, path = {}", self.path)
    }
}

/// Holds configuration of a single physical host or a set of identical hosts.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct HostConfig {
    /// Host name.
    /// Should be set if count = 1.
    pub name: Option<String>,
    /// Host name prefix.
    /// Full name is produced by appending host instance number to the prefix.
    /// Should be set if count > 1.
    pub name_prefix: Option<String>,
    /// Host CPU capacity.
    pub cpus: u32,
    /// Host memory capacity in GB.
    pub memory: u64,
    /// Number of such hosts.
    pub count: Option<u32>,
}

/// Holds configuration of a single scheduler or a set of identically configured schedulers.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct SchedulerConfig {
    /// Scheduler name.
    /// Should be set if count = 1.
    pub name: Option<String>,
    /// Scheduler name prefix.
    /// Full name is produced by appending scheduler instance number to the prefix.
    /// Should be set if count > 1.
    pub name_prefix: Option<String>,
    /// VM placement algorithm used by scheduler(s).
    pub algorithm: String,
    /// Number of such schedulers.
    pub count: Option<u32>,
}

/// Represents simulation configuration.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct SimulationConfig {
    /// Period length in seconds for sending statistics from host to monitoring.
    pub send_stats_period: f64,
    /// Message delay in seconds for communications via network.
    pub message_delay: f64,
    /// Period is seconds for waiting before retrying failed allocation request.
    pub allocation_retry_period: f64,
    /// VM start duration in seconds.
    pub vm_start_duration: f64,
    /// VM stop duration in seconds.
    pub vm_stop_duration: f64,
    /// Whether to schedule VMs based on real resource utilization instead of allocated resources.
    pub allow_vm_overcommit: bool,
    /// Network throughput in GB/s.
    /// Currently used to compute VM migration duration.
    pub network_throughput: u64,
    /// Length of simulation in seconds (for public datasets only).
    pub simulation_length: f64,
    /// Duration in seconds between simulation steps.
    pub step_duration: f64,
    /// Timeout in seconds after which unallocated VM becomes failed.
    pub vm_allocation_timeout: f64,
    /// Used VM trace dataset.
    pub trace: Option<VmDatasetConfig>,
    /// Configurations of physical hosts.
    pub hosts: Vec<HostConfig>,
    /// Configurations of VM schedulers.
    pub schedulers: Vec<SchedulerConfig>,
}

impl SimulationConfig {
    /// Creates simulation config by reading parameter values from YAM file
    /// (uses default values if some parameters are absent).
    pub fn from_file(file_name: &str) -> Self {
        let raw: RawSimulationConfig = serde_yaml::from_str(
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
