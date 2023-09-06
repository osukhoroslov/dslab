use std::time::Instant;

use clap::Parser;

use dslab_core::log_info;
use dslab_core::simulation::Simulation;
use dslab_iaas::core::config::sim_config::SimulationConfig;
use dslab_iaas::simulation::CloudSimulation;

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
    #[clap(long, short)]
    config: String,
}

fn main() {
    init_logger();
    let args = Args::parse();
    let config = SimulationConfig::from_file(&args.config);

    let initialization_start = Instant::now();
    let sim = Simulation::new(123);
    let mut cloud_sim = CloudSimulation::new(sim, config.clone());
    log_info!(
        cloud_sim.context(),
        "Simulation init time: {:.2?}",
        initialization_start.elapsed()
    );

    let simulation_start = Instant::now();
    let mut accumulated_cpu_rate = 0.;
    let mut num_of_iterations = 0;
    loop {
        cloud_sim.step_for_duration(config.step_duration);

        let cpu_allocation_rate = cloud_sim.cpu_allocation_rate();
        let memory_allocation_rate = cloud_sim.memory_allocation_rate();
        let average_cpu_load = cloud_sim.average_cpu_load();
        let average_memory_load = cloud_sim.average_memory_load();
        log_info!(
            cloud_sim.context(),
            concat!(
                "CPU allocation rate: {:.2?}, memory allocation rate: {:.2?},",
                " CPU average load: {:.2?}, memory average load: {:.2?}"
            ),
            cpu_allocation_rate,
            memory_allocation_rate,
            average_cpu_load,
            average_memory_load
        );
        accumulated_cpu_rate += cloud_sim.cpu_allocation_rate();
        num_of_iterations += 1;

        if cloud_sim.current_time() > config.simulation_length {
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
        "Average CPU allocation rate is {:.1}%",
        100. * accumulated_cpu_rate / (num_of_iterations as f64)
    );
}
