use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::process;
use std::time::Instant;
use std::rc::Rc;
use sugars::{rc};

extern crate env_logger;
use serde::{Deserialize, Serialize};

use cloud_plugin::config::SimulationConfig;
use cloud_plugin::load_model::ConstLoadModel;
use cloud_plugin::resource_pool::Allocation;
use cloud_plugin::simulation::CloudSimulation;
use cloud_plugin::vm::VirtualMachine;
use cloud_plugin::vm_placement_algorithm::FirstFit;
use core::log_info;
use core::simulation::Simulation;

pub static HOST_CPU_CAPACITY: f64 = 1000.;
pub static HOST_MEMORY_CAPACITY: f64 = 1000.;
pub static SIMULATION_LENGTH: f64 = 100.;
pub static NUMBER_OF_HOSTS: u32 = 10000;
pub static MAX_VMS_IN_SIMULATION: u32 = 50000;
pub static TIME_MARGIN: f64 = 7200.; // due to simulation begins ~7200 hours before the two weeks period
pub static BLOCK_STEPS: u64 = 10000;

fn init_logger() {
    use std::io::Write;
    use env_logger::Builder;
    Builder::from_default_env()
          .format(|buf, record| writeln!(buf, "{}", record.args()))
          .init();
}

#[allow(non_snake_case)]
#[derive(Serialize, Deserialize, Debug)]
struct VMType {
    id: String,
    vmTypeId: String,
    core: f64,
    memory: f64,
}

#[allow(non_snake_case)]
#[derive(Serialize, Deserialize, Debug)]
struct VMInstance {
    vmId: String,
    vmTypeId: String,
    starttime: f64,
    endtime: Option<f64>,
}

struct SimulationDatacet {
    vm_types: HashMap<String, VMType>,
    vm_instances: Vec<VMInstance>,
    current_vm: usize,
    sim_config: Rc<SimulationConfig>,
}

struct VMRequest {
    alloc: Allocation,
    vm: VirtualMachine,
    start_time: f64
}

impl SimulationDatacet {
    pub fn new(sim_config: Rc<SimulationConfig>) -> Self {
        Self {
            vm_types: HashMap::new(),
            vm_instances: Vec::new(),
            current_vm: 0,
            sim_config: sim_config.clone()
        }
    }

    pub fn get_next_vm(&mut self) -> Option<VMRequest> {
        if self.current_vm >= self.vm_instances.len() {
            return None;
        }

        let raw_vm = &self.vm_instances[self.current_vm];
        let vm_params = self.vm_types.get(&raw_vm.vmTypeId).unwrap();
        let cpu_usage = (HOST_CPU_CAPACITY * vm_params.core) as u32;
        let memory_usage = (HOST_MEMORY_CAPACITY * vm_params.memory) as u64;
        self.current_vm += 1;

        let lifetime = raw_vm.endtime.unwrap_or(SIMULATION_LENGTH) - raw_vm.starttime;
        Some(VMRequest {
            alloc: Allocation {
                id: raw_vm.vmId.clone(),
                cpu_usage,
                memory_usage,
            },
            vm: VirtualMachine::new(
                lifetime,
                Box::new(ConstLoadModel::new(1.0)),
                Box::new(ConstLoadModel::new(1.0)),
                self.sim_config.clone(),
            ),
            start_time: raw_vm.starttime,
        })
    }
}

fn parse_vm_types(file_name: &str) -> Result<HashMap<String, VMType>, Box<dyn Error>> {
    let mut result: HashMap<String, VMType> = HashMap::new();

    let mut rdr = csv::Reader::from_reader(File::open(file_name)?);
    for record in rdr.deserialize() {
        let vm_type: VMType = record?;
        result.insert(vm_type.vmTypeId.clone(), vm_type);
    }
    Ok(result)
}

fn parse_vm_instances(file_name: &str, instances_count: u32) -> Result<Vec<VMInstance>, Box<dyn Error>> {
    let mut result: Vec<VMInstance> = Vec::new();

    let mut rdr = csv::Reader::from_reader(File::open(file_name)?);
    let mut count = 0;
    for record in rdr.deserialize() {
        let vm_instance: VMInstance = record?;
        count += 1;
        if count >= instances_count {
            break;
        }
        result.push(vm_instance);
    }
    result.sort_by(|a, b| a.starttime.partial_cmp(&b.starttime).unwrap());
    Ok(result)
}

fn parse_dataset(
        sim_config: SimulationConfig,
        vm_types_file_name: &str,
        vm_instances_file_name: &str,
        instnces_count: u32
) -> SimulationDatacet {
    let mut result = SimulationDatacet::new(rc!(sim_config));

    let vm_types_or_error = parse_vm_types(vm_types_file_name);
    if vm_types_or_error.is_err() {
        println!("error parsing VM types: {}", vm_types_or_error.err().unwrap());
        process::exit(1);
    }
    result.vm_types = vm_types_or_error.unwrap();

    let vm_instances_or_error = parse_vm_instances(vm_instances_file_name, instnces_count);
    if vm_instances_or_error.is_err() {
        println!("error parsing VM instances: {}", vm_instances_or_error.err().unwrap());
        process::exit(1);
    }
    result.vm_instances = vm_instances_or_error.unwrap();
    result
}

fn simulation_with_traces(
    vm_types_file_name: &str,
    vm_instances_file_name: &str,
    instnces_count: u32,
    sim_config: SimulationConfig,
) {
    let initialization_start = Instant::now();
    let sim = Simulation::new(123);
    let mut cloud_sim = CloudSimulation::new(sim, sim_config.clone());

    let mut dataset = parse_dataset(sim_config.clone(), vm_types_file_name, vm_instances_file_name, instnces_count);

    for i in 1..NUMBER_OF_HOSTS {
        let host_name = &format!("h{}", i);
        cloud_sim.add_host(host_name, HOST_CPU_CAPACITY as u32, HOST_MEMORY_CAPACITY as u64);
    }
    cloud_sim.add_scheduler("s", Box::new(FirstFit::new()));

    loop {
        let request_opt = dataset.get_next_vm();
        if request_opt.is_none() {
            break;
        }
        let request = request_opt.unwrap();

        cloud_sim.spawn_vm_with_delay(
            &request.alloc.id,
            request.alloc.cpu_usage,
            request.alloc.memory_usage,
            request.vm.lifetime(),
            Box::new(ConstLoadModel::new(1.0)),
            Box::new(ConstLoadModel::new(1.0)),
            "s",
            request.start_time + TIME_MARGIN,
        );
    }

    log_info!(cloud_sim.get_context(), "Simulation init time: {:.2?}", initialization_start.elapsed());
    let mut current_block = 0;
    let simulation_start = Instant::now();

    loop {
        cloud_sim.steps(BLOCK_STEPS);

        let mut sum_cpu_load = 0.;
        let mut sum_memory_load = 0.;
        let ctx = cloud_sim.get_context();
        for i in 1..NUMBER_OF_HOSTS {
            let host_name = &format!("h{}", i);
            sum_cpu_load += cloud_sim.host(host_name).borrow().get_cpu_load(ctx.time());
            sum_memory_load += cloud_sim.host(host_name).borrow().get_memory_load(ctx.time());
        }
        current_block += 1;

        log_info!(
            ctx,
            "CPU allocation rate: {}, memory allocation rate: {}", sum_cpu_load, sum_memory_load
        );
        if sum_cpu_load == 0. && current_block > 5 {
            break;
        }
    }

    log_info!(cloud_sim.get_context(), "Simulation process time {:.2?}", simulation_start.elapsed());
    log_info!(cloud_sim.get_context(), "Total events processed {}", cloud_sim.event_count());
    log_info!(cloud_sim.get_context(), 
              "Events per second {}",
              cloud_sim.event_count() as u128 / simulation_start.elapsed().as_millis() * 1000);
}

fn main() {
    init_logger();
    let config = SimulationConfig::from_file("config.yaml");
    simulation_with_traces(
        "vm_types.csv",
        "vm_instances.csv",
        MAX_VMS_IN_SIMULATION,
        config.clone(),
    );
}
