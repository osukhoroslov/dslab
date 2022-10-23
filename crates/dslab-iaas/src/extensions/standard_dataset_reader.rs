//! Dataset reader for manually created datasets.

use std::fs::File;

use crate::core::load_model::ConstantLoadModel;
use crate::extensions::dataset_reader::DatasetReader;
use crate::extensions::dataset_reader::VMRequestInternal;

/// Dataset reader for manually created datasets.
///
/// Pass the produced JSON file to [`parse()`](StandardDatasetReader::parse) method.
pub struct StandardDatasetReader {
    vm_requests: Vec<VMRequestInternal>,
    current_vm: usize,
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
            self.vm_requests.push(VMRequestInternal {
                id: None,
                cpu_usage: raw_vm["cpu_usage"].as_u64().unwrap() as u32,
                memory_usage: raw_vm["memory_usage"].as_u64().unwrap(),
                lifetime: raw_vm["lifetime"].as_f64().unwrap(),
                start_time: raw_vm["arrival_time"].as_f64().unwrap(),
                cpu_load_model: Box::new(ConstantLoadModel::new(1.)),
                memory_load_model: Box::new(ConstantLoadModel::new(1.)),
                scheduler_name: Some(raw_vm["scheduler"].to_string()),
            });
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
