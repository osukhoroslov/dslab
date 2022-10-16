//! Simulation configuration.

use serde::{Deserialize, Serialize};

use crate::core::load_model::LoadModelType;
use crate::core::vm_placement_algorithm::VmPlacementAlgorithmType;

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
    /// number of hosts in datacenter (for public datasets only)
    pub number_of_hosts: Option<u32>,
    /// CPU capacity for default host
    pub host_cpu_capacity: Option<f64>,
    /// RAM capacity for default host
    pub host_memory_capacity: Option<f64>,
    /// duration beetween user access the simulation info
    pub step_duration: Option<f64>,
    /// VM becomes failed after this timeout is reached
    pub vm_allocation_timeout: Option<f64>,
    /// Cloud infrastructure: hosts, schedulers, incoming VMs
    pub infrastructure: Option<ConfigInfrastructure>,
}

/// Represents custom load model in .ymal config.
///
/// model_type: type of custom resourse load model
/// args: arbitrary arguments for load model
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct ConfigLoadModel {
    pub model_type: LoadModelType,
    pub args: String
}

/// Represents incoming virtual machine request.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct ConfigVM {
    /// requested CPU capacity for VM
    pub cpu_usage: u32,
    /// requested memory capacity for VM
    pub memory_usage: u64,
    /// VM lifetime
    pub lifetime: f64,
    /// CPU load model - it`s type and arguments
    pub cpu_load_model: ConfigLoadModel,
    /// memory load model - it`s type and arguments
    pub memory_load_model: ConfigLoadModel,
    /// explicitly defined VM id, if not selected - it will be generated automaticly
    pub vm_id: Option<u32>,
    /// scheduler name where VM should be scheduled
    pub scheduler_name: String,
    /// simulation time when current VM arrives to scheduler
    pub delay: f64,
    /// number of such VMs
    pub amount: u32,
}

/// Represents custom virtual machine placement algorithm in .ymal config.
///
/// algorithm_type: type of algorithm
/// args: arbitrary arguments for the algorithm
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct ConfigVmPlacementAlgorithm {
    pub algorithm_type: VmPlacementAlgorithmType,
    pub args: String
}

/// Represents physical host properties.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct ConfigScheduler {
    /// scheduler name
    pub name: String,
    /// VM placement algorithm for this scheduler
    pub placement_algorithm: ConfigVmPlacementAlgorithm,
    /// number of such schedulers
    pub amount: u32,
}

/// Represents physical host properties.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct ConfigHost {
    /// host name
    pub name: String,
    /// host CPU capacity
    pub cpu_capacity: u32,
    /// host memory capacity
    pub memory_capacity: u64,
    /// number of such hosts
    pub amount: u32,
}

/// Represents cloud infrustructure for simulation instance.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct ConfigInfrastructure {
    /// incoming VM requests
    pub vms: Vec<ConfigVM>,
    /// cloud physical hosts
    pub hosts: Vec<ConfigHost>,
    /// cloud schedulers
    pub schedulers: Vec<ConfigScheduler>,
}

/// Represents simulation configuration.
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
    /// number of hosts in datacenter (for public datasets only)
    pub number_of_hosts: u32,
    /// CPU capacity for default host
    pub host_cpu_capacity: f64,
    /// RAM capacity for default host
    pub host_memory_capacity: f64,
    /// duration beetween user access the simulation info
    pub step_duration: f64,
    /// VM becomes failed after this timeout is reached
    pub vm_allocation_timeout: f64,
    /// Cloud infrastructure: hosts, schedulers, incoming VMs
    pub infrastructure: ConfigInfrastructure,
}

impl SimulationConfig {
    /// Creates simulation config with default parameter values.
    pub fn new() -> Self {
        Self {
            send_stats_period: 0.5,
            message_delay: 0.2,
            allocation_retry_period: 1.0,
            vm_start_duration: 1.,
            vm_stop_duration: 0.5,
            allow_vm_overcommit: false,
            network_throughput: 1,
            simulation_length: 0.,
            number_of_hosts: 1,
            host_cpu_capacity: 1.,
            host_memory_capacity: 1.,
            step_duration: 500.,
            vm_allocation_timeout: 50.,
            infrastructure: ConfigInfrastructure{ vms: Vec::new(), hosts: Vec::new(), schedulers: Vec::new() },
        }
    }

    /// Creates simulation config by reading parameter values from .yaml file (uses default values if some parameters are absent).
    pub fn from_file(file_name: &str) -> Self {
        let data: SimulationConfigRaw =
            serde_yaml::from_str(&std::fs::read_to_string(file_name).expect(&format!("Can't read file {}", file_name)))
                .expect(&format!("Can't parse YAML from file {}", file_name));
        let default = SimulationConfig::new();
        Self {
            send_stats_period: data.send_stats_period.unwrap_or(default.send_stats_period),
            message_delay: data.message_delay.unwrap_or(default.message_delay),
            allocation_retry_period: data.allocation_retry_period.unwrap_or(default.allocation_retry_period),
            vm_start_duration: data.vm_start_duration.unwrap_or(default.vm_start_duration),
            vm_stop_duration: data.vm_stop_duration.unwrap_or(default.vm_stop_duration),
            allow_vm_overcommit: data.allow_vm_overcommit.unwrap_or(default.allow_vm_overcommit),
            network_throughput: data.network_throughput.unwrap_or(default.network_throughput),
            simulation_length: data.simulation_length.unwrap_or(default.simulation_length),
            number_of_hosts: data.number_of_hosts.unwrap_or(default.number_of_hosts),
            host_cpu_capacity: data.host_cpu_capacity.unwrap_or(default.host_cpu_capacity),
            host_memory_capacity: data.host_memory_capacity.unwrap_or(default.host_memory_capacity),
            step_duration: data.step_duration.unwrap_or(default.step_duration),
            vm_allocation_timeout: data.vm_allocation_timeout.unwrap_or(default.vm_allocation_timeout),
            infrastructure: data.infrastructure.unwrap_or(default.infrastructure),
        }
    }
}
