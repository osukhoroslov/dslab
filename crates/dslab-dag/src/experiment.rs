//! Tool for running multiple experiments.

use std::cell::RefCell;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use itertools::Itertools;
use serde::{Deserialize, Serialize};
use threadpool::ThreadPool;

use crate::dag::DAG;
use crate::dag_simulation::DagSimulation;
use crate::data_item::DataTransferMode;
use crate::network::{read_network_config, NetworkConfig};
use crate::resource::{read_resources, ResourceConfig};
use crate::runner::Config;
use crate::scheduler::Scheduler;
use crate::scheduler_resolver::SchedulerParams;

/// Contains result of one run.
#[derive(Serialize, Debug)]
pub struct RunResult {
    pub dag: String,
    pub system: String,
    pub scheduler: String,
    pub makespan: f64,
}

#[derive(Deserialize)]
struct ExperimentConfig {
    dags: Vec<PathBuf>,
    systems: Vec<PathBuf>,
    schedulers: Vec<String>,
    data_transfer_mode: DataTransferMode,
}

struct Run {
    dag_name: String,
    dag: DAG,
    system_name: String,
    resources: Vec<ResourceConfig>,
    network: NetworkConfig,
    scheduler: SchedulerParams,
}

pub struct Experiment {
    runs: Vec<Run>,
    data_transfer_mode: DataTransferMode,
    scheduler_resolver: fn(&SchedulerParams) -> Option<Rc<RefCell<dyn Scheduler>>>,
}

impl Experiment {
    /// Load config from a file.
    pub fn load(
        config_path: &str,
        scheduler_resolver: fn(&SchedulerParams) -> Option<Rc<RefCell<dyn Scheduler>>>,
    ) -> Self {
        let config: ExperimentConfig = std::fs::read_to_string(config_path)
            .ok()
            .and_then(|f| serde_yaml::from_str(&f).ok())
            .unwrap_or_else(|| panic!("Can't read config from file {}", config_path));

        let dags = get_all_files(&config.dags).into_iter().map(|path| {
            (
                path.file_name().unwrap().to_str().unwrap().to_string(),
                DAG::from_file(&path),
            )
        });

        let systems = get_all_files(&config.systems)
            .into_iter()
            .map(|path| {
                (
                    path.file_name().unwrap().to_str().unwrap().to_string(),
                    read_resources(&path),
                    read_network_config(&path),
                )
            })
            .inspect(|(file_name, resources, _network)| {
                if resources.is_empty() {
                    panic!("Can't have empty list of resources: {file_name}");
                }
            })
            .inspect(|(file_name, _resources, network)| {
                if network.make_network().is_none() {
                    panic!("Unknown network model in {file_name}: {network:?}");
                }
            });

        let schedulers = config
            .schedulers
            .into_iter()
            .map(|s| SchedulerParams::from_str(&s).unwrap_or_else(|| panic!("Can't parse scheduler params from {s}")))
            .inspect(|params| {
                if scheduler_resolver(params).is_none() {
                    panic!("Can't parse scheduler params from params {params:?}")
                }
            });

        let runs = dags
            .cartesian_product(systems)
            .cartesian_product(schedulers.into_iter())
            .map(
                |(((dag_name, dag), (system_name, resources, network)), scheduler)| Run {
                    dag_name,
                    dag,
                    system_name,
                    resources,
                    network,
                    scheduler,
                },
            )
            .collect::<Vec<_>>();

        Self {
            runs,
            data_transfer_mode: config.data_transfer_mode,
            scheduler_resolver,
        }
    }

    /// Run all experiments.
    pub fn run(self, num_threads: usize) -> Vec<RunResult> {
        let total_runs = self.runs.len();

        let finished_runs = Arc::new(AtomicUsize::new(0));
        let result = Arc::new(Mutex::new(Vec::new()));

        let pool = ThreadPool::new(num_threads);
        let start_time = Instant::now();
        for run in self.runs.into_iter() {
            let finished_runs = finished_runs.clone();
            let result = result.clone();
            pool.execute(move || {
                let network = run.network.make_network().unwrap();
                let scheduler = (self.scheduler_resolver)(&run.scheduler).unwrap();

                let mut sim = DagSimulation::new(
                    123,
                    network,
                    scheduler,
                    Config {
                        data_transfer_mode: self.data_transfer_mode,
                    },
                );
                for resource in run.resources.into_iter() {
                    sim.add_resource(&resource.name, resource.speed, resource.cores, resource.memory);
                }

                sim.init(run.dag);
                sim.step_until_no_events();

                let makespan = sim.time();

                result.lock().unwrap().push(RunResult {
                    dag: run.dag_name,
                    system: run.system_name,
                    scheduler: format!("{}", run.scheduler),
                    makespan,
                });

                finished_runs.fetch_add(1, Ordering::SeqCst);
                let finished = finished_runs.load(Ordering::SeqCst);

                let elapsed = start_time.elapsed();
                let remaining =
                    Duration::from_secs_f64(elapsed.as_secs_f64() / finished as f64 * (total_runs - finished) as f64);
                print!("\r{}", " ".repeat(70));
                print!(
                    "\rFinished {}/{} [{}%] runs in {:.2?}, remaining time: {:.2?}",
                    finished,
                    total_runs,
                    (finished as f64 * 100. / total_runs as f64).round() as i32,
                    elapsed,
                    remaining
                );
                std::io::stdout().flush().unwrap();
            });
        }

        pool.join();

        print!("\r{}", " ".repeat(70));
        println!("\rFinished {} runs in {:.2?}", total_runs, start_time.elapsed());

        let mut result = Arc::try_unwrap(result).unwrap().into_inner().unwrap();
        result.sort_by(|a, b| a.makespan.total_cmp(&b.makespan));
        result
    }
}

fn get_all_files(paths: &[PathBuf]) -> Vec<PathBuf> {
    let mut result = Vec::new();
    for path in paths.iter() {
        if Path::new(&path).is_dir() {
            result.extend(get_all_files(
                &std::fs::read_dir(path)
                    .unwrap()
                    .map(|s| s.unwrap().path())
                    .collect::<Vec<_>>(),
            ));
        } else {
            result.push(path.clone());
        }
    }
    result
}
