//! Simulation configuration.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

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
    /// cloud physical hosts
    pub hosts: Option<Vec<HostConfig>>,
    /// cloud schedulers
    pub schedulers: Option<Vec<SchedulerConfig>>,
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
    pub count: Option<u32>,
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
    /// cloud physical hosts
    pub hosts: Vec<HostConfig>,
    /// cloud schedulers
    pub schedulers: Vec<SchedulerConfig>,
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
            hosts: Vec::new(),
            schedulers: Vec::new(),
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
            hosts: data.hosts.unwrap_or(Vec::new()),
            schedulers: data.schedulers.unwrap_or(Vec::new()),
        }
    }
}

/// Parses options string from config value. Example: parsing string "threshold=0.8,cpu=8" returns
/// map with two varaibles for threshold and CPU.
pub fn parse_options(config_str: &str) -> HashMap<String, String> {
    let mut result: HashMap<String, String> = HashMap::new();

    let variables = config_str.split(",");
    for variable in variables {
        let split = variable.split("=").collect::<Vec<&str>>();
        result.insert(split.get(0).unwrap().to_string(), split.get(1).unwrap().to_string());
    }
    result
}

/// Parses raw model config string, which consists of two parts - name and arguments.
/// Example: ConstLoadModel[load=0.8] parts are name ConstLoadModel and arguments string "load=0.8".
pub fn parse_model_name_and_args(config_str: &str) -> (String, String) {
    let cleanup = config_str.replace("]", "").replace("\"", "");
    let split = cleanup.split("[").collect::<Vec<&str>>();
    let model_type = split.get(0).unwrap();
    let model_args = split.get(1).unwrap().to_string();
    (model_type.to_string(), model_args)
}
