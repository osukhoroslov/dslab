use std::collections::HashMap;
use std::fs::File;

use log::info;
use serde::{Deserialize, Serialize};

use crate::extensions::dataset_reader::DatasetReader;
use crate::extensions::dataset_reader::VMRequest;

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

pub struct HuaweiDatasetReader {
    simulation_length: f64,

    vm_events: Vec<VMEvent>,
    vm_requests: Vec<VMRequest>,
    current_vm: usize,
}

impl HuaweiDatasetReader {
    pub fn new(simulation_length: f64) -> Self {
        Self {
            simulation_length,
            vm_events: Vec::new(),
            vm_requests: Vec::new(),
            current_vm: 0,
        }
    }

    fn parse_vm_events(&mut self, file_name: &str) {
        let mut rdr = csv::Reader::from_reader(File::open(file_name).unwrap());
        for record in rdr.deserialize() {
            let vm_event: VMEvent = record.unwrap();

            if vm_event.is_finish == 0 && vm_event.time > self.simulation_length {
                continue;
            }
            self.vm_events.push(vm_event);
        }

        info!("Got {} active VMs", self.vm_events.len() / 2);
    }

    pub fn parse(&mut self, vm_events_file_name: &str) {
        self.parse_vm_events(vm_events_file_name);

        let mut end_times = HashMap::new();
        for event in &self.vm_events {
            if event.is_finish == 1 {
                end_times.insert(event.vm_id, event.time);
            }
        }

        for event in &self.vm_events {
            if event.is_finish == 0 {
                self.vm_requests.push(VMRequest {
                    id: event.vm_id,
                    cpu_usage: event.cpu,
                    memory_usage: event.memory,
                    lifetime: end_times.get(&event.vm_id).unwrap_or(&self.simulation_length) - event.time,
                    start_time: event.time,
                });
            }
        }
    }
}

impl DatasetReader for HuaweiDatasetReader {
    fn get_next_vm(&mut self) -> Option<VMRequest> {
        loop {
            if self.current_vm >= self.vm_requests.len() {
                return None;
            }
            self.current_vm += 1;

            return Some(self.vm_requests[self.current_vm - 1].clone());
        }
    }
}
