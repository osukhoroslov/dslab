use log::info;
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;
use std::time::Instant;

use cloud_plugin::core::config::SimulationConfig;
use cloud_plugin::core::load_model::ConstLoadModel;
use cloud_plugin::core::vm_placement_algorithm::BestFit;
use cloud_plugin::simulation::CloudSimulation;
use simcore::simulation::Simulation;

const NUM_HOSTS: u64 = 1000;
const NUM_VMS: u64 = 5000;
const CPU_CAPACITY: u32 = 144;
const RAM_CAPACITY: u64 = 4096;

fn init_logger() {
    use env_logger::Builder;
    use std::io::Write;
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();
}

fn simulation(sim_config: SimulationConfig) {
    let simulation_start = Instant::now();

    let sim = Simulation::new(123);
    let mut cloud_sim = CloudSimulation::new(sim, sim_config);

    let s = cloud_sim.add_scheduler("s", Box::new(BestFit::new()));

    for i in 1..NUM_HOSTS {
        cloud_sim.add_host(&format!("host{}", i), CPU_CAPACITY, RAM_CAPACITY);
    }

    for i in 1..NUM_VMS {
        let mut rng = StdRng::seed_from_u64(47);

        let vm_cpu_distribution = [1, 2, 4, 8, 16];
        let vm_ram_distribution = [128, 256, 512];

        cloud_sim.spawn_vm_now(
            i as u32,
            vm_cpu_distribution[(rng.gen::<u32>() as usize) % 5],
            vm_ram_distribution[(rng.gen::<u32>() as usize) % 3],
            100.0,
            Box::new(ConstLoadModel::new(1.0)),
            Box::new(ConstLoadModel::new(1.0)),
            s,
        );
    }

    cloud_sim.step_for_duration(10.);
    info!("Elapsed time is {} seconds", simulation_start.elapsed().as_secs_f64());
}

fn main() {
    init_logger();
    let config = SimulationConfig::from_file("config.yaml");
    simulation(config.clone());
}
