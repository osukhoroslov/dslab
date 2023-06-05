use std::time::Instant;

use clap::Parser;

use dslab_core::log_info;
use dslab_core::simulation::Simulation;
use dslab_iaas::core::config::SimulationConfig;
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
    #[clap(short, long)]
    config_name: String,
}

fn main() {
    init_logger();

    let args = Args::parse();
    let config_name = &args.config_name;
    let sim_config = SimulationConfig::from_file(config_name);

    let initialization_start = Instant::now();
    let sim = Simulation::new(123);
    let mut cloud_sim = CloudSimulation::new(sim, sim_config.clone());

    log_info!(
        cloud_sim.context(),
        "Simulation init time: {:.2?}",
        initialization_start.elapsed()
    );
    let simulation_start = Instant::now();

    let mut accumulated_cpu_utilization = 0.;
    let mut num_of_iterations = 0;
    loop {
        cloud_sim.step_for_duration(*sim_config.step_duration);

        let mut sum_cpu_load = 0.;
        let mut sum_memory_load = 0.;
        let mut sum_cpu_allocated = 0.;
        let mut sum_memory_allocated = 0.;
        for i in 1..(*sim_config.number_of_hosts + 1) {
            let host_name = format!("h{}", i);
            sum_cpu_load += cloud_sim
                .host_by_name(&host_name)
                .borrow()
                .get_cpu_load(cloud_sim.context().time());
            sum_memory_load += cloud_sim
                .host_by_name(&host_name)
                .borrow()
                .get_memory_load(cloud_sim.context().time());
            sum_cpu_allocated += cloud_sim.host_by_name(&host_name).borrow().get_cpu_allocated();
            sum_memory_allocated += cloud_sim.host_by_name(&host_name).borrow().get_memory_allocated();
        }

        log_info!(
            cloud_sim.context(),
            concat!(
                "CPU allocation rate: {:.2?}, memory allocation rate: {:.2?},",
                " CPU load rate: {:.2?}, memory load rate: {:.2?}"
            ),
            sum_cpu_allocated / (*sim_config.host_cpu_capacity * *sim_config.number_of_hosts as f64),
            sum_memory_allocated / (*sim_config.host_memory_capacity * *sim_config.number_of_hosts as f64),
            sum_cpu_load / (*sim_config.number_of_hosts as f64),
            sum_memory_load / (*sim_config.number_of_hosts as f64)
        );
        accumulated_cpu_utilization += sum_cpu_load / (*sim_config.number_of_hosts as f64);
        num_of_iterations += 1;

        if cloud_sim.current_time() > *sim_config.simulation_length {
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
