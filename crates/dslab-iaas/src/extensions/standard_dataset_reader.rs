//! Dataset reader for standard JSON format.

use std::fs::File;

use serde::{Deserialize, Serialize};

use crate::core::load_model::load_model_resolver;
use crate::extensions::dataset_reader::DatasetReader;
use crate::extensions::dataset_reader::VMRequest;

/// Dataset reader for standard JSON format.
///
/// This format can be used for storing arbitrary VM request traces,
/// i.e. manually created, generated or exported from existing cloud traces.
///
/// Example: see `examples/iaas/workload.json`.
///
/// Pass the needed JSON file to [`parse()`](StandardDatasetReader::parse) method.
pub struct StandardDatasetReader {
    vm_requests: Vec<VMRequest>,
    current_vm: usize,
}

/// Represents allocation request from dataset.
#[derive(Clone, Serialize, Deserialize, Debug)]
struct StandardVmRequest {
    pub id: Option<u32>,
    pub cpu_usage: u32,
    pub memory_usage: u64,
    pub lifetime: f64,
    pub arrival_time: f64,
    pub cpu_load_model: String,
    pub memory_load_model: String,
    pub scheduler: Option<String>,
    pub count: Option<u32>,
}

impl StandardDatasetReader {
    /// Creates dataset reader.
    pub fn new() -> Self {
        Self {
            vm_requests: Vec::new(),
            current_vm: 0,
        }
    }

    /// Loads the dataset from JSON file with VM requests.
    pub fn parse(&mut self, vms_file_name: &str) {
        let file = File::open(vms_file_name).unwrap();
        let raw_json: Vec<serde_json::Value> = serde_json::from_reader(file).unwrap();

        for raw_vm in raw_json.iter() {
            let dataset_request: StandardVmRequest = serde_json::from_str(&raw_vm.to_string()).unwrap();
            for _i in 0..dataset_request.count.unwrap_or(1) {
                self.vm_requests.push(VMRequest {
                    id: dataset_request.id,
                    cpu_usage: dataset_request.cpu_usage,
                    memory_usage: dataset_request.memory_usage,
                    lifetime: dataset_request.lifetime,
                    start_time: dataset_request.arrival_time,
                    cpu_load_model: load_model_resolver(dataset_request.cpu_load_model.clone()),
                    memory_load_model: load_model_resolver(dataset_request.memory_load_model.clone()),
                    scheduler_name: dataset_request.scheduler.clone(),
                });
            }
        }
    }
}

impl DatasetReader for StandardDatasetReader {
    fn get_next_vm(&mut self) -> Option<VMRequest> {
        if self.current_vm >= self.vm_requests.len() {
            return None;
        }
        self.current_vm += 1;

        return Some(self.vm_requests[self.current_vm - 1].clone());
    }
}
