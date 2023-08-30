use std::cell::RefCell;
use std::rc::Rc;
use std::time::Instant;

use clap::Parser;
use indexmap::IndexMap;
use log::Level;

use dslab_iaas::core::config::exp_config::ExperimentConfig;
use dslab_iaas::experiments::{Experiment, SimulationCallbacks};
use dslab_iaas::simulation::CloudSimulation;

fn init_logger(level: Level) {
    use env_logger::Builder;
    use std::io::Write;
    Builder::from_default_env()
        .filter_level(level.to_level_filter())
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();
}

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

#[derive(Clone)]
pub struct SimulationControlCallbacks {
    pub step: u32,
}

impl SimulationControlCallbacks {
    fn new() -> Self {
        Self { step: 0 }
    }
}

impl SimulationCallbacks for SimulationControlCallbacks {
    fn on_simulation_start(&mut self, _sim: Rc<RefCell<CloudSimulation>>) {
        self.step = 0;
    }

    fn on_step(&mut self, sim: Rc<RefCell<CloudSimulation>>) -> bool {
        self.step += 1;
        if self.step % 10000 == 0 {
            let average_cpu_load = sim.borrow_mut().average_cpu_load();
            let average_memory_load = sim.borrow_mut().average_memory_load();
            sim.borrow_mut().log_info(format!(
                "Step = {}, Average CPU load = {}, average memory load = {}",
                self.step, average_cpu_load, average_memory_load
            ));
        }

        true
    }

    fn on_simulation_finish(&mut self, sim: Rc<RefCell<CloudSimulation>>) -> IndexMap<String, String> {
        let end_time = sim.borrow_mut().current_time();

        let vm_count = sim.borrow().vm_api().borrow().vms.len();
        let mut total_energy_consumed = 0.;
        for (_, host) in sim.borrow().hosts() {
            total_energy_consumed += host.borrow_mut().get_energy_consumed(end_time);
        }

        sim.borrow_mut()
            .log_info(format!("Total energy consumed by hosts: {:.2}", total_energy_consumed));

        let mut result = IndexMap::new();
        result.insert("energy consumed".to_string(), format!("{}", total_energy_consumed));
        result.insert("VMs allocated".to_string(), format!("{}", vm_count));
        result
    }
}

fn main() {
    let args = Args::parse();
    init_logger(args.log_level);
    let simulation_start = Instant::now();

    let sim_config = ExperimentConfig::from_file(&args.config);
    let mut exp = Experiment::new(
        Box::new(SimulationControlCallbacks::new()),
        sim_config,
        args.threads,
        args.log_dir,
        args.log_level,
    );
    exp.run();

    println!("Simulation process time {:.2?}", simulation_start.elapsed());
}
