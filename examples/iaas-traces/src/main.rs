use std::str::FromStr;
use std::time::Instant;

use clap::Parser;
use log::warn;

use dslab_core::log_info;
use dslab_core::simulation::Simulation;
use dslab_iaas::core::config::sim_config::SimulationConfig;
use dslab_iaas::core::vm_placement_algorithm::VMPlacementAlgorithm;
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

    let scheduler_id = cloud_sim.add_scheduler("s", VMPlacementAlgorithm::single(BestFit::new()));

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

        log_info!(
            cloud_sim.context(),
            concat!(
                "CPU allocation rate: {:.2?}, memory allocation rate: {:.2?},",
                " CPU load rate: {:.2?}, memory load rate: {:.2?}"
            ),
            cloud_sim.clone().cpu_allocation_rate(),
            cloud_sim.clone().memory_allocation_rate(),
            cloud_sim.clone().average_cpu_load(),
            cloud_sim.clone().average_memory_load()
        );
        accumulated_cpu_utilization += cloud_sim.cpu_allocation_rate();
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
            config.hosts.get(0).unwrap().cpus as f64,
            config.hosts.get(0).unwrap().memory as f64,
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
