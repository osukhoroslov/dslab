use std::fs;

use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct SimulationConfigData {
    pub send_stats_period: f64,       // periodically send statistics from host to monitoring
    pub message_delay: f64,           // message trip time from any host to any direction
    pub allocation_retry_period: f64, // when allocation request fails then wait for ...
    pub vm_start_duration: f64,       // vm initialization duration
    pub vm_stop_duration: f64,        // vm deallocation duration
    pub allow_vm_overcommit: bool,    // pack VM by real resource consumption, not SLA
}

pub struct SimulationConfig {
    pub data: SimulationConfigData,
}

impl SimulationConfig {
    pub fn new() -> Self {
        Self {
            data: SimulationConfigData {
                send_stats_period: 0.5,
                message_delay: 0.2,
                allocation_retry_period: 1.0,
                vm_start_duration: 1.,
                vm_stop_duration: 0.5,
                allow_vm_overcommit: false,
            },
        }
    }

    pub fn from_file(file_name: &str) -> Self {
        let raw_data = fs::read_to_string(file_name);
        if raw_data.is_err() {
            println!(
                "error parsing simulation config: {}, return default",
                raw_data.err().unwrap()
            );
            return SimulationConfig::new();
        }
        let data = serde_yaml::from_str(&raw_data.unwrap());
        if data.is_err() {
            println!(
                "error parsing simulation config: {}, return default",
                data.err().unwrap()
            );
            return SimulationConfig::new();
        }

        Self { data: data.unwrap() }
    }
}
