//! Dataset reader for Huawei VM Placements Dataset (2021).

use std::collections::HashMap;
use std::fs::File;

use log::info;
use serde::{Deserialize, Serialize};

use crate::core::load_model::ConstantLoadModel;
use crate::extensions::dataset_reader::DatasetReader;
use crate::extensions::dataset_reader::VMRequest;

/// Represents allocation or deallocation request from dataset.
#[derive(Serialize, Deserialize, Debug)]
struct VMEvent {
    #[serde(rename = "vmid")]
    vm_id: u32,
    #[serde(rename = "cpu")]
    cpu: u32,
    #[serde(rename = "memory")]
    memory: u64,
    #[serde(rename = "time")]
    time: f64,
    #[serde(rename = "type")]
    is_finish: u32,
}

/// Dataset reader for
/// [Huawei VM Placements Dataset](https://github.com/huaweicloud/VM-placement-dataset/blob/main/Huawei-East-1/data/Huawei-East-1.csv).
///
/// Pass the downloaded CSV file to [`parse()`](HuaweiDatasetReader::parse) method.
pub struct HuaweiDatasetReader {
    simulation_length: f64,

    vm_events: Vec<VMEvent>,
    vm_requests: Vec<VMRequest>,
    current_vm: usize,
}

impl HuaweiDatasetReader {
    /// Creates dataset reader.
    ///
    /// Reads only the VMs started within first `simulation_length` seconds.
    pub fn new(simulation_length: f64) -> Self {
        Self {
            simulation_length,
            vm_events: Vec::new(),
            vm_requests: Vec::new(),
            current_vm: 0,
        }
    }

    /// Loads the dataset from the original CSV file.
    pub fn parse(&mut self, vm_events_file_name: String) {
        let mut reader = csv::Reader::from_reader(File::open(vm_events_file_name).unwrap());
        let mut active_vms_count = 0;
        let mut end_times = HashMap::new();

        for record in reader.deserialize() {
            let vm_event: VMEvent = record.unwrap();
            if vm_event.time > self.simulation_length {
                continue;
            }
            if vm_event.is_finish == 0 {
                active_vms_count += 1;
            }
            if vm_event.is_finish == 1 {
                end_times.insert(vm_event.vm_id, vm_event.time);
            }
            self.vm_events.push(vm_event);
        }
        info!("Read {} VM instances", active_vms_count);

        for event in &self.vm_events {
            if event.is_finish == 0 {
                self.vm_requests.push(VMRequest {
                    id: Some(event.vm_id),
                    cpu_usage: event.cpu,
                    memory_usage: event.memory,
                    lifetime: end_times.get(&event.vm_id).unwrap_or(&self.simulation_length) - event.time,
                    start_time: event.time,
                    cpu_load_model: Box::new(ConstantLoadModel::new(1.)),
                    memory_load_model: Box::new(ConstantLoadModel::new(1.)),
                    scheduler_name: None,
                });
            }
        }
    }
}

impl DatasetReader for HuaweiDatasetReader {
    fn get_next_vm(&mut self) -> Option<VMRequest> {
        if self.current_vm >= self.vm_requests.len() {
            return None;
        }
        self.current_vm += 1;

        return Some(self.vm_requests[self.current_vm - 1].clone());
    }
}
