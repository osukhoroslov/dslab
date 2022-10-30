//! Dataset reader for Azure Trace for Packing 2020.

use std::collections::HashMap;
use std::fs::File;

use log::info;
use serde::{Deserialize, Serialize};

use crate::core::load_model::ConstantLoadModel;
use crate::extensions::dataset_reader::DatasetReader;
use crate::extensions::dataset_reader::VMRequest;

/// Represents information about VM type from the dataset.
///
/// Note that CPU and memory sizes of VMs are stored as fractions of host capacities.
#[allow(non_snake_case)]
#[derive(Serialize, Deserialize, Debug)]
struct VMType {
    id: String,
    #[serde(rename = "vmTypeId")]
    vm_type_id: String,
    core: f64,
    memory: f64,
}

/// Represents information about VM instance from the dataset.
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

/// Dataset reader for
/// [Azure Trace for Packing 2020](https://github.com/Azure/AzurePublicDataset/blob/master/AzureTracesForPacking2020.md).
///
/// Requires the conversion of original sqlite files to CVS format as follows:
///
/// ```shell
/// $ sqlite3 packing_trace_zone_a_v1.sqlite
/// sqlite> .headers on
/// sqlite> .mode csv
/// sqlite> .output vm_instances.csv
/// sqlite> SELECT vmId, vmTypeId, starttime, endtime FROM vm ORDER BY starttime;
/// sqlite> .quit
///
/// $ sqlite3 packing_trace_zone_a_v1.sqlite
/// sqlite> .headers on
/// sqlite> .mode csv
/// sqlite> .output vm_types.csv
/// sqlite> SELECT id, vmTypeId, core, memory FROM vmType;
/// sqlite> .quit
/// ```
///
/// Pass the produced CSV files to [`parse()`](AzureDatasetReader::parse) method.
pub struct AzureDatasetReader {
    simulation_length: f64,
    host_cpu_capacity: f64,
    host_memory_capacity: f64,

    vm_types: HashMap<String, VMType>,
    vm_instances: Vec<VMInstance>,
    current_vm: usize,
}

impl AzureDatasetReader {
    /// Creates dataset reader.
    ///
    /// The sizes of VMs are computed based on the provided host CPU and memory capacities.
    /// Reads only the VMs started within first `simulation_length` seconds.
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

    /// Loads the dataset from CSV files with VM types and instances.
    pub fn parse(&mut self, vm_types_file_name: String, vm_instances_file_name: String) {
        self.parse_vm_types(vm_types_file_name);
        self.parse_vm_instances(vm_instances_file_name);
    }

    /// Parses CSV file with VM types.
    fn parse_vm_types(&mut self, file_name: String) {
        let mut reader = csv::Reader::from_reader(File::open(file_name).unwrap());
        for record in reader.deserialize() {
            let vm_type: VMType = record.unwrap();
            self.vm_types.insert(vm_type.vm_type_id.clone(), vm_type);
        }
    }

    /// Parses CSV file with VM instances.
    fn parse_vm_instances(&mut self, file_name: String) {
        let mut reader = csv::Reader::from_reader(File::open(file_name).unwrap());
        for record in reader.deserialize() {
            let vm_instance: VMInstance = record.unwrap();
            if vm_instance.start_time < 0. {
                continue;
            }
            if vm_instance.start_time * 86400. > self.simulation_length {
                break;
            }
            self.vm_instances.push(vm_instance);
        }

        info!("Read {} VM instances", self.vm_instances.len());
    }
}

impl DatasetReader for AzureDatasetReader {
    fn get_next_vm(&mut self) -> Option<VMRequest> {
        if self.current_vm >= self.vm_instances.len() {
            return None;
        }

        let raw_vm = self.vm_instances.get(self.current_vm).unwrap();
        let start_time = raw_vm.start_time.max(0.) * 86400.;
        let vm_params = self.vm_types.get(&raw_vm.vm_type_id).unwrap();
        let cpu_usage = (self.host_cpu_capacity * vm_params.core) as u32;
        let memory_usage = (self.host_memory_capacity * vm_params.memory) as u64;
        self.current_vm += 1;

        let end_time = raw_vm
            .end_time
            .map(|t| t * 86400.)
            .unwrap_or(self.simulation_length)
            .min(self.simulation_length);
        let lifetime = end_time - start_time;
        return Some(VMRequest {
            id: Some(raw_vm.vm_id.clone()),
            cpu_usage,
            memory_usage,
            lifetime,
            start_time,
            cpu_load_model: Box::new(ConstantLoadModel::new(1.)),
            memory_load_model: Box::new(ConstantLoadModel::new(1.)),
            scheduler_name: None,
        });
    }
}
