use std::cell::RefCell;
use std::rc::Rc;
use std::time::Instant;

use clap::Parser;

use dslab_core::log_info;
use dslab_iaas::core::config::exp_config::ExperimentConfig;
use dslab_iaas::experiments::{Experiment, SimulationCallbacks};
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
    /// Path to simulation config
    #[clap(short, long)]
    config: String,

    /// Number of threads to use (default - use all available cores)
    #[clap(short, long, default_value_t = std::thread::available_parallelism().unwrap().get())]
    threads: usize,

    /// Parallel mode on
    #[clap(short, long, default_value_t = false)]
    parallel_mode: bool,
}

fn number_of_threads(threads: usize, parallel_mode: bool) -> Option<usize> {
    if !parallel_mode {
        return None;
    }
    Some(threads)
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
            log_info!(
                sim.borrow().context(),
                "Step = {}, Average CPU load = {}, average memory load = {}",
                self.step,
                average_cpu_load,
                average_memory_load
            );
        }

        true
    }

    fn on_simulation_finish(&mut self, sim: Rc<RefCell<CloudSimulation>>) {
        let end_time = sim.borrow_mut().current_time();
        log_info!(
            sim.borrow().context(),
            "Total energy consumed by host one: {:.2}",
            sim.borrow()
                .host_by_name("h1")
                .borrow_mut()
                .get_energy_consumed(end_time)
        );
        log_info!(
            sim.borrow().context(),
            "Total energy consumed by host two: {:.2}",
            sim.borrow()
                .host_by_name("h2")
                .borrow_mut()
                .get_energy_consumed(end_time)
        );
    }
}

fn main() {
    init_logger();

    let args = Args::parse();
    let simulation_start = Instant::now();

    let sim_config = ExperimentConfig::from_file(&args.config);
    let mut exp = Experiment::new(
        Box::new(SimulationControlCallbacks::new()),
        sim_config,
        number_of_threads(args.threads, args.parallel_mode),
    );
    exp.run();

    println!("Simulation process time {:.2?}", simulation_start.elapsed());
}
