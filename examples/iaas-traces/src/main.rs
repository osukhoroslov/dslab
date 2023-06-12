use std::time::Instant;

use clap::Parser;

use dslab_core::log_info;
use dslab_iaas::core::config::SimulationConfig;
use dslab_iaas::experiments::{Experiment, OnTestCaseFinishedCallback};
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
    config: String,
}

pub struct PrintEnergyCallback {}

impl OnTestCaseFinishedCallback for PrintEnergyCallback {
    fn on_experiment_finish(&self, sim: &mut CloudSimulation) {
        let end_time = sim.current_time();
        log_info!(sim.context(), "==== New test case ====");
        log_info!(
            sim.context(),
            "Total energy consumed by host one: {:.2}",
            sim.host_by_name("h1").borrow_mut().get_energy_consumed(end_time)
        );
        log_info!(
            sim.context(),
            "Total energy consumed by host two: {:.2}",
            sim.host_by_name("h2").borrow_mut().get_energy_consumed(end_time)
        );
    }
}

fn main() {
    init_logger();

    let args = Args::parse();
    let simulation_start = Instant::now();

    let sim_config = SimulationConfig::from_file(&args.config);
    let mut exp = Experiment::new(Box::new(PrintEnergyCallback {}), sim_config);
    exp.start();

    println!("Simulation process time {:.2?}", simulation_start.elapsed());
}
