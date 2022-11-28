use std::time::Instant;

use clap::Parser;
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;

use dslab_core::log_info;
use dslab_core::simulation::Simulation;
use dslab_iaas::core::config::SimulationConfig;
use dslab_iaas::core::load_model::ConstantLoadModel;
use dslab_iaas::core::vm_placement_algorithms::first_fit::FirstFit;
use dslab_iaas::simulation::CloudSimulation;

const CPU_CAPACITY: u32 = 144;
const RAM_CAPACITY: u64 = 204800;

fn init_logger() {
    use env_logger::Builder;
    use std::io::Write;
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(long)]
    num_hosts: u32,

    #[clap(long)]
    num_vms: u32,
}

fn simulation(sim_config: SimulationConfig) {
    let args = Args::parse();

    let simulation_start = Instant::now();

    let sim = Simulation::new(123);
    let mut cloud_sim = CloudSimulation::new(sim, sim_config);

    let s = cloud_sim.add_scheduler("s", Box::new(FirstFit::new()));

    for i in 1..=args.num_hosts {
        cloud_sim.add_host(&format!("host{}", i), CPU_CAPACITY, RAM_CAPACITY);
    }

    let mut rng = StdRng::seed_from_u64(47);
    let vm_cpu_distribution = [1, 2, 4, 8];
    let vm_ram_distribution = [512, 1024, 2048];

    for _ in 1..=args.num_vms {
        cloud_sim.spawn_vm_now(
            vm_cpu_distribution[rng.gen_range(0..4)],
            vm_ram_distribution[rng.gen_range(0..3)],
            100.0,
            Box::new(ConstantLoadModel::new(1.0)),
            Box::new(ConstantLoadModel::new(1.0)),
            None,
            s,
        );
    }

    cloud_sim.step_for_duration(10.);

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
    let config = SimulationConfig::from_file("config.yaml");
    simulation(config);
}
