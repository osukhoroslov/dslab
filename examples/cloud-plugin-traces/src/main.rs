use std::time::Instant;

use cloud_plugin::core::config::SimulationConfig;
use cloud_plugin::core::vm_placement_algorithm::FirstFit;
use cloud_plugin::extensions::azure_dataset_reader::AzureDatasetReader;
use cloud_plugin::extensions::dataset_reader::DatasetReader;
use cloud_plugin::extensions::huawei_dataset_reader::HuaweiDatasetReader;
use cloud_plugin::simulation::CloudSimulation;
use simcore::log_info;
use simcore::simulation::Simulation;

pub static NUMBER_OF_HOSTS: u32 = 3000;
pub static HOST_CPU_CAPACITY: f64 = 192.;
pub static HOST_MEMORY_CAPACITY: f64 = 320.;
pub static SIMULATION_LENGTH: f64 = 8640.;
pub static STEP_DURATION: f64 = 600.;

fn init_logger() {
    use env_logger::Builder;
    use std::io::Write;
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();
}

#[derive(PartialEq)]
pub enum DatasetType {
    Azure,
    Huawei,
}

fn simulation_with_traces(sim_config: SimulationConfig, dataset: &mut dyn DatasetReader) {
    let initialization_start = Instant::now();
    let sim = Simulation::new(123);
    let mut cloud_sim = CloudSimulation::new(sim, sim_config.clone());

    let mut hosts: Vec<u32> = Vec::new();
    for i in 1..sim_config.number_of_hosts {
        let host_name = &format!("h{}", i);
        let host_id = cloud_sim.add_host(host_name, HOST_CPU_CAPACITY as u32, HOST_MEMORY_CAPACITY as u64);
        hosts.push(host_id);
    }
    let scheduler_id = cloud_sim.add_scheduler("s", Box::new(FirstFit::new()));

    log_info!(
        cloud_sim.context(),
        "Simulation init time: {:.2?}",
        initialization_start.elapsed()
    );
    let simulation_start = Instant::now();
    cloud_sim.spawn_vms_from_dataset(scheduler_id, dataset);

    loop {
        cloud_sim.step_for_duration(STEP_DURATION);

        let mut sum_cpu_load = 0.;
        let mut sum_memory_load = 0.;
        let mut sum_cpu_allocated = 0.;
        let mut sum_memory_allocated = 0.;
        for host_id in &hosts {
            sum_cpu_load += cloud_sim
                .host(*host_id)
                .borrow()
                .get_cpu_load(cloud_sim.context().time());
            sum_memory_load += cloud_sim
                .host(*host_id)
                .borrow()
                .get_memory_load(cloud_sim.context().time());
            sum_cpu_allocated += cloud_sim.host(*host_id).borrow().get_cpu_allocated();
            sum_memory_allocated += cloud_sim.host(*host_id).borrow().get_memory_allocated();
        }

        log_info!(
            cloud_sim.context(),
            concat!(
                "CPU allocation rate: {:.2?}, memory allocation rate: {:.2?},",
                " CPU load rate: {:.2?}, memory load rate: {:.2?}"
            ),
            sum_cpu_allocated / (HOST_CPU_CAPACITY * hosts.len() as f64),
            sum_memory_allocated / (HOST_MEMORY_CAPACITY * hosts.len() as f64),
            sum_cpu_load / (hosts.len() as f64),
            sum_memory_load / (hosts.len() as f64)
        );
        if cloud_sim.current_time() > sim_config.simulation_length {
            break;
        }
    }

    log_info!(
        cloud_sim.context(),
        "Simulation process time {:.2?}",
        simulation_start.elapsed()
    );
    log_info!(
        cloud_sim.context(),
        "Total events processed {}",
        cloud_sim.event_count()
    );
    log_info!(
        cloud_sim.context(),
        "Events per second {:.0}",
        cloud_sim.event_count() as f64 / simulation_start.elapsed().as_secs_f64()
    );
}

fn main() {
    init_logger();
    let dataset_type = DatasetType::Huawei;

    if dataset_type == DatasetType::Azure {
        let config = SimulationConfig::from_file("azure.yaml");
        let mut dataset = AzureDatasetReader::new(config.simulation_length, HOST_CPU_CAPACITY, HOST_MEMORY_CAPACITY);
        dataset.parse("vm_types.csv", "vm_instances.csv");
        simulation_with_traces(config.clone(), &mut dataset);
    } else if dataset_type == DatasetType::Huawei {
        let config = SimulationConfig::from_file("huawei.yaml");
        let mut dataset = HuaweiDatasetReader::new(config.simulation_length);
        dataset.parse("Huawei-East-1.csv");
        simulation_with_traces(config.clone(), &mut dataset);
    }
}
