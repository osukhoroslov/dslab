//! Dataset reader for manually created datasets.

use std::fs::File;

use serde::{Deserialize, Serialize};

use crate::core::load_model::parse_load_model;
use crate::extensions::dataset_reader::DatasetReader;
use crate::extensions::dataset_reader::VMRequestInternal;

/// Dataset reader for manually created datasets.
///
/// Pass the produced JSON file to [`parse()`](StandardDatasetReader::parse) method.
pub struct StandardDatasetReader {
    vm_requests: Vec<VMRequestInternal>,
    current_vm: usize,
}

/// Represents allocation request from dataset.
#[derive(Clone, Serialize, Deserialize, Debug)]
struct VmRequest {
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
            let dataset_request: VmRequest = serde_json::from_str(&raw_vm.to_string()).unwrap();
            for _i in 0..dataset_request.count.unwrap_or(1) {
                self.vm_requests.push(VMRequestInternal {
                    id: None,
                    cpu_usage: dataset_request.clone().cpu_usage,
                    memory_usage: dataset_request.clone().memory_usage,
                    lifetime: dataset_request.clone().lifetime,
                    start_time: dataset_request.clone().arrival_time,
                    cpu_load_model: parse_load_model(dataset_request.clone().cpu_load_model),
                    memory_load_model: parse_load_model(dataset_request.clone().memory_load_model),
                    scheduler_name: dataset_request.clone().scheduler,
                });
            }
        }
    }
}

impl DatasetReader for StandardDatasetReader {
    fn get_next_vm(&mut self) -> Option<VMRequestInternal> {
        if self.current_vm >= self.vm_requests.len() {
            return None;
        }
        self.current_vm += 1;

        return Some(self.vm_requests[self.current_vm - 1].clone());
    }
}
