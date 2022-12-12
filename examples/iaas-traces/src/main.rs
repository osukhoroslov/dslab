use std::str::FromStr;
use std::time::Instant;

use clap::Parser;
use log::warn;

use dslab_core::log_info;
use dslab_core::simulation::Simulation;
use dslab_iaas::core::config::SimulationConfig;
use dslab_iaas::core::vm_placement_algorithms::best_fit::BestFit;
use dslab_iaas::extensions::azure_dataset_reader::AzureDatasetReader;
use dslab_iaas::extensions::dataset_reader::DatasetReader;
use dslab_iaas::extensions::huawei_dataset_reader::HuaweiDatasetReader;
use dslab_iaas::simulation::CloudSimulation;

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

impl FromStr for DatasetType {
    type Err = ();
    fn from_str(input: &str) -> Result<DatasetType, Self::Err> {
        if input != "azure" && input != "huawei" {
            warn!("Cannot parse dataset type, use azure as default");
        }
        match input {
            "azure" => Ok(DatasetType::Azure),
            "huawei" => Ok(DatasetType::Huawei),
            _ => Ok(DatasetType::Azure),
        }
    }
}

fn simulation_with_traces(sim_config: SimulationConfig, dataset: &mut dyn DatasetReader) {
    let initialization_start = Instant::now();
    let sim = Simulation::new(123);
    let mut cloud_sim = CloudSimulation::new(sim, sim_config.clone());

    let mut hosts: Vec<u32> = Vec::new();
    for i in 1..sim_config.number_of_hosts + 1 {
        let host_name = &format!("h{}", i);
        let host_id = cloud_sim.add_host(
            host_name,
            sim_config.host_cpu_capacity as u32,
            sim_config.host_memory_capacity as u64,
        );
        hosts.push(host_id);
    }
    let scheduler_id = cloud_sim.add_scheduler("s", Box::new(BestFit::new()));

    log_info!(
        cloud_sim.context(),
        "Simulation init time: {:.2?}",
        initialization_start.elapsed()
    );
    let simulation_start = Instant::now();
    cloud_sim.spawn_vms_from_dataset(scheduler_id, dataset);

    let mut accumulated_cpu_utilization = 0.;
    let mut num_of_iterations = 0;
    loop {
        cloud_sim.step_for_duration(sim_config.step_duration);

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
            sum_cpu_allocated / (sim_config.host_cpu_capacity * hosts.len() as f64),
            sum_memory_allocated / (sim_config.host_memory_capacity * hosts.len() as f64),
            sum_cpu_load / (hosts.len() as f64),
            sum_memory_load / (hosts.len() as f64)
        );
        accumulated_cpu_utilization += sum_cpu_load / (hosts.len() as f64);
        num_of_iterations += 1;

        if cloud_sim.current_time() > sim_config.simulation_length {
            break;
        }
    }

    println!("Simulation process time {:.2?}", simulation_start.elapsed());
    println!("Total events processed {}", cloud_sim.event_count());
    println!(
        "Events per second {:.0}",
        cloud_sim.event_count() as f64 / simulation_start.elapsed().as_secs_f64()
    );
    println!(
        "Mean CPU utilization is {:.1}%",
        100. * accumulated_cpu_utilization / (num_of_iterations as f64)
    );
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(long)]
    dataset_type: String,

    #[clap(long)]
    dataset_path: Option<String>,
}

fn main() {
    init_logger();
    let args = Args::parse();
    let dataset_type = DatasetType::from_str(&args.dataset_type).unwrap();
    let dataset_path = args.dataset_path.unwrap_or_else(|| ".".to_string());

    if dataset_type == DatasetType::Azure {
        let config = SimulationConfig::from_file("azure.yaml");
        let mut dataset = AzureDatasetReader::new(
            config.simulation_length,
            config.host_cpu_capacity,
            config.host_memory_capacity,
        );
        dataset.parse(
            format!("{}/vm_types.csv", dataset_path),
            format!("{}/vm_instances.csv", dataset_path),
        );
        simulation_with_traces(config, &mut dataset);
    } else if dataset_type == DatasetType::Huawei {
        let config = SimulationConfig::from_file("huawei.yaml");
        let mut dataset = HuaweiDatasetReader::new(config.simulation_length);
        dataset.parse(format!("{}/Huawei-East-1.csv", dataset_path));
        simulation_with_traces(config, &mut dataset);
    }
}
