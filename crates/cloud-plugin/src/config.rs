use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct SimulationConfig {
    pub send_stats_period: f64,       // periodically send statistics from host to monitoring
    pub message_delay: f64,           // message trip time from any host to any direction
    pub allocation_retry_period: f64, // when allocation request fails then wait for ...
    pub vm_start_duration: f64,       // vm initialization duration
    pub vm_stop_duration: f64,        // vm deallocation duration
    pub allow_vm_overcommit: bool,    // pack VM by real resource consumption, not SLA
    pub network_throughput: u64,      // to define VM migration duration
}

impl SimulationConfig {
    pub fn new() -> Self {
        Self {
            send_stats_period: 0.5,
            message_delay: 0.2,
            allocation_retry_period: 1.0,
            vm_start_duration: 1.,
            vm_stop_duration: 0.5,
            allow_vm_overcommit: false,
            network_throughput: 1,
        }
    }

    pub fn from_file(file_name: &str) -> Self {
        let data: SimulationConfig =
            serde_yaml::from_str(&std::fs::read_to_string(file_name).expect(&format!("Can't read file {}", file_name)))
                .expect(&format!("Can't parse YAML from file {}", file_name));
        Self {
            send_stats_period: data.send_stats_period,
            message_delay: data.message_delay,
            allocation_retry_period: data.allocation_retry_period,
            vm_start_duration: data.vm_start_duration,
            vm_stop_duration: data.vm_stop_duration,
            allow_vm_overcommit: data.allow_vm_overcommit,
            network_throughput: data.network_throughput,
        }
    }
}
