use std::time::Instant;

use clap::Parser;
use indexmap::IndexMap;
use log::Level;

use dslab_iaas::core::config::exp_config::ExperimentConfig;
use dslab_iaas::experiment::{Experiment, SimulationCallbacks};
use dslab_iaas::simulation::CloudSimulation;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Path to simulation config
    #[clap(long)]
    config: String,

    /// Number of threads to use (default - use all available cores)
    #[clap(long, default_value_t = std::thread::available_parallelism().unwrap().get())]
    threads: usize,

    /// Path to save simulation log. If none then stdout is used
    #[clap(long)]
    log_dir: Option<String>,

    /// Log level: one of Error, Warn, Info, Debug, Trace
    #[clap(long, default_value_t = Level::Info)]
    log_level: Level,
}

fn main() {
    let args = Args::parse();
    init_logger(args.log_level);
    let simulation_start = Instant::now();

    let sim_config = ExperimentConfig::from_file(&args.config);
    let mut exp = Experiment::new(
        sim_config,
        Box::new(ExperimentCallbacks::new()),
        args.log_dir,
        args.log_level,
    );
    exp.run(args.threads);

    println!("Simulation process time {:.2?}", simulation_start.elapsed());
}

fn init_logger(level: Level) {
    use env_logger::Builder;
    use std::io::Write;
    Builder::from_default_env()
        .filter_level(level.to_level_filter())
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();
}

#[derive(Clone)]
pub struct ExperimentCallbacks {
    pub step: u32,
}

impl ExperimentCallbacks {
    fn new() -> Self {
        Self { step: 0 }
    }
}

impl SimulationCallbacks for ExperimentCallbacks {
    fn on_simulation_start(&mut self, _sim: &mut CloudSimulation) {
        self.step = 0;
    }

    fn on_step(&mut self, sim: &mut CloudSimulation) -> bool {
        self.step += 1;
        if self.step % 10000 == 0 {
            let cpu_rate = sim.cpu_allocation_rate();
            let memory_rate = sim.memory_allocation_rate();
            sim.log_info(format!(
                "CPU allocation rate = {}, memory allocation rate = {}",
                cpu_rate, memory_rate
            ));
        }
        true
    }

    fn on_simulation_finish(&mut self, sim: &mut CloudSimulation) -> IndexMap<String, String> {
        let mut result = IndexMap::new();
        result.insert(
            "total_vm_count".to_string(),
            sim.vm_api().borrow().get_vm_count().to_string(),
        );
        result.insert("cpu_allocation_rate".to_string(), sim.cpu_allocation_rate().to_string());
        result.insert(
            "memory_allocation_rate".to_string(),
            sim.memory_allocation_rate().to_string(),
        );
        result.insert("average_cpu_load".to_string(), sim.average_cpu_load().to_string());
        result.insert("average_memory_load".to_string(), sim.average_memory_load().to_string());

        let mut total_energy_consumed = 0.;
        let end_time = sim.current_time();
        for (_, host) in sim.hosts() {
            total_energy_consumed += host.borrow_mut().get_energy_consumed(end_time);
        }
        sim.log_info(format!("Total energy consumed: {:.2}", total_energy_consumed));
        result.insert("total_energy_consumed".to_string(), total_energy_consumed.to_string());

        result
    }
}
