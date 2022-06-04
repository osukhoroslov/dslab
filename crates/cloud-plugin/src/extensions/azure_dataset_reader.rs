use std::collections::HashMap;
use std::fs::File;

use log::info;
use serde::{Deserialize, Serialize};

use crate::extensions::dataset_reader::DatasetReader;
use crate::extensions::dataset_reader::VMRequest;

#[allow(non_snake_case)]
#[derive(Serialize, Deserialize, Debug)]
struct VMType {
    id: String,
    #[serde(rename = "vmTypeId")]
    vm_type_id: String,
    core: f64,
    memory: f64,
}

#[derive(Serialize, Deserialize, Debug)]
struct VMInstance {
    #[serde(rename = "vmId")]
    vm_id: u32,
    #[serde(rename = "vmTypeId")]
    vm_type_id: String,
    #[serde(rename = "starttime")]
    start_time: f64,
    #[serde(rename = "endtime")]
    end_time: Option<f64>,
}

pub struct AzureDatasetReader {
    simulation_length: f64,
    host_cpu_capacity: f64,
    host_memory_capacity: f64,

    vm_types: HashMap<String, VMType>,
    vm_instances: Vec<VMInstance>,
    current_vm: usize,
}

impl AzureDatasetReader {
    pub fn new(simulation_length: f64, host_cpu_capacity: f64, host_memory_capacity: f64) -> Self {
        Self {
            simulation_length,
            host_cpu_capacity,
            host_memory_capacity,
            vm_types: HashMap::new(),
            vm_instances: Vec::new(),
            current_vm: 0,
        }
    }

    fn parse_vm_types(&mut self, file_name: &str) {
        let mut rdr = csv::Reader::from_reader(File::open(file_name).unwrap());
        for record in rdr.deserialize() {
            let vm_type: VMType = record.unwrap();
            self.vm_types.insert(vm_type.vm_type_id.clone(), vm_type);
        }
    }

    fn parse_vm_instances(&mut self, file_name: &str) {
        let mut rdr = csv::Reader::from_reader(File::open(file_name).unwrap());
        for record in rdr.deserialize() {
            let vm_instance: VMInstance = record.unwrap();

            if vm_instance.start_time < 0. {
                continue;
            }
            if vm_instance.start_time * 86400. > self.simulation_length {
                break;
            }
            self.vm_instances.push(vm_instance);
        }

        info!("Got {} active VMs", self.vm_instances.len());
    }

    pub fn parse(&mut self, vm_types_file_name: &str, vm_instances_file_name: &str) {
        self.parse_vm_types(vm_types_file_name);
        self.parse_vm_instances(vm_instances_file_name);
    }
}

impl DatasetReader for AzureDatasetReader {
    fn get_next_vm(&mut self) -> Option<VMRequest> {
        loop {
            if self.current_vm >= self.vm_instances.len() {
                return None;
            }

            let raw_vm = &self.vm_instances[self.current_vm];
            let start_time = raw_vm.start_time.max(0.) * 86400.;
            let vm_params = self.vm_types.get(&raw_vm.vm_type_id).unwrap();
            let cpu_usage = (self.host_cpu_capacity * vm_params.core) as u32;
            let memory_usage = (self.host_memory_capacity * vm_params.memory) as u64;
            self.current_vm += 1;

            let end_time = raw_vm.end_time.map(|t| t * 86400.).unwrap_or(self.simulation_length);
            let lifetime = end_time - start_time;
            return Some(VMRequest {
                id: raw_vm.vm_id.clone(),
                cpu_usage,
                memory_usage,
                lifetime,
                start_time,
            });
        }
    }
}
