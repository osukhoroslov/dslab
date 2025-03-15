//! Tool for running experiments across many (dag, system. scheduler) combinations.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::str::FromStr;
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
use crate::parsers::config::ParserConfig;
use crate::resource::{read_resource_configs, ResourceConfig};
use crate::run_stats::RunStats;
use crate::runner::Config;
use crate::scheduler::{RcScheduler, SchedulerParams};

/// Contains result of a single simulation run.
#[derive(Serialize, Deserialize, Debug)]
pub struct RunResult {
    pub dag: String,
    pub system: String,
    pub scheduler: String,
    pub completed: bool,
    pub makespan: f64,
    pub exec_time: f64,
    pub run_stats: RunStats,
}

#[derive(Deserialize)]
struct ExperimentConfig {
    dags: Vec<PathBuf>,
    #[serde(default)]
    dag_parser: ParserConfig,
    systems: Vec<PathBuf>,
    schedulers: Vec<String>,
    data_transfer_mode: DataTransferMode,
    traces_dir: Option<String>,
    billing_interval: Option<f64>,
}

struct Run {
    dag_name: String,
    dag: DAG,
    system_name: String,
    resources: Vec<ResourceConfig>,
    network: NetworkConfig,
    scheduler: SchedulerParams,
}

/// Represents experiment consisting of multiple simulation runs,
/// where each run corresponds to a unique (dag, system. scheduler) combination.
pub struct Experiment {
    runs: Vec<Run>,
    data_transfer_mode: DataTransferMode,
    scheduler_resolver: fn(&SchedulerParams) -> Option<RcScheduler>,
    traces_dir: Option<String>,
    billing_interval: Option<f64>,
}

impl Experiment {
    /// Creates new experiment.
    pub fn new(
        dags: Vec<(String, DAG)>,
        systems: Vec<(String, Vec<ResourceConfig>, NetworkConfig)>,
        data_transfer_mode: DataTransferMode,
        schedulers: Vec<SchedulerParams>,
        scheduler_resolver: fn(&SchedulerParams) -> Option<RcScheduler>,
        traces_dir: Option<String>,
        billing_interval: Option<f64>,
    ) -> Self {
        let runs = dags
            .into_iter()
            .cartesian_product(systems)
            .cartesian_product(schedulers)
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
            data_transfer_mode,
            scheduler_resolver,
            traces_dir,
            billing_interval,
        }
    }

    /// Loads experiment from YAML config file.
    pub fn load<P: AsRef<Path>>(
        config_path: P,
        scheduler_resolver: fn(&SchedulerParams) -> Option<RcScheduler>,
    ) -> Self {
        let config: ExperimentConfig = std::fs::read_to_string(config_path.as_ref())
            .ok()
            .and_then(|f| serde_yaml::from_str(&f).ok())
            .unwrap_or_else(|| panic!("Can't read config from file {}", config_path.as_ref().display()));

        let dags_paths: Vec<PathBuf> = config
            .dags
            .iter()
            .map(|path| config_path.as_ref().parent().unwrap().join(path))
            .collect();
        let dags = get_all_files(&dags_paths).into_iter().map(|path| {
            (
                path.file_stem().unwrap().to_str().unwrap().to_string(),
                DAG::from_file(&path, &config.dag_parser),
            )
        });

        let systems_paths: Vec<PathBuf> = config
            .systems
            .iter()
            .map(|path| config_path.as_ref().parent().unwrap().join(path))
            .collect();
        let systems = get_all_files(&systems_paths).into_iter().map(|path| {
            let file_name = path.file_name().unwrap().to_str().unwrap().to_string();
            let file_stem = path.file_stem().unwrap().to_str().unwrap().to_string();
            let resources = read_resource_configs(&path);
            assert!(
                !resources.is_empty(),
                "Can't have empty list of resources: {}",
                file_name
            );
            (file_stem, resources, read_network_config(&path))
        });

        let schedulers = config
            .schedulers
            .into_iter()
            .map(|s| {
                SchedulerParams::from_str(&s).unwrap_or_else(|e| panic!("Can't parse scheduler params from {s}: {e}"))
            })
            .inspect(|params| {
                if scheduler_resolver(params).is_none() {
                    panic!("Can't resolve scheduler from params {params:?}")
                }
            });

        let runs = dags
            .cartesian_product(systems)
            .cartesian_product(schedulers)
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

        let traces_dir = config
            .traces_dir
            .map(|path| String::from(config_path.as_ref().parent().unwrap().join(path).to_string_lossy()));

        Self {
            runs,
            data_transfer_mode: config.data_transfer_mode,
            scheduler_resolver,
            traces_dir,
            billing_interval: config.billing_interval,
        }
    }

    /// Runs experiment and returns its results.
    pub fn run(self, num_threads: usize) -> Vec<RunResult> {
        if let Some(dir) = &self.traces_dir {
            std::fs::create_dir_all(dir).unwrap();
        }

        let total_runs = self.runs.len();

        let finished_run_atomic = Arc::new(AtomicUsize::new(0));
        let results = Arc::new(Mutex::new(Vec::new()));

        let pool = ThreadPool::new(num_threads);
        let start_time = Instant::now();
        for run in self.runs.into_iter() {
            let finished_run_atomic = finished_run_atomic.clone();
            let results = results.clone();
            let traces_dir = self.traces_dir.clone();
            pool.execute(move || {
                let scheduler = (self.scheduler_resolver)(&run.scheduler).unwrap();

                let mut sim = DagSimulation::new(
                    123,
                    run.resources,
                    run.network,
                    scheduler,
                    Config {
                        data_transfer_mode: self.data_transfer_mode,
                        billing_interval: self.billing_interval.unwrap_or(1.0),
                    },
                );

                let runner = sim.init(run.dag);
                if traces_dir.is_some() {
                    runner.borrow_mut().enable_trace_log(true);
                }

                let start = Instant::now();
                sim.step_until_no_events();
                let exec_time = start.elapsed().as_secs_f64();
                let makespan = sim.time();

                runner.borrow().validate_completed();
                if let Some(dir) = traces_dir {
                    runner
                        .borrow()
                        .trace_log()
                        .save_to_file(&format!(
                            "{}/{}_{}_{}.json",
                            dir, &run.dag_name, &run.system_name, &run.scheduler,
                        ))
                        .unwrap();
                }

                results.lock().unwrap().push(RunResult {
                    dag: run.dag_name,
                    system: run.system_name,
                    scheduler: run.scheduler.to_string(),
                    completed: runner.borrow().is_completed(),
                    makespan,
                    exec_time,
                    run_stats: runner.borrow().run_stats().clone(),
                });

                finished_run_atomic.fetch_add(1, Ordering::SeqCst);
                let finished_runs = finished_run_atomic.load(Ordering::SeqCst);

                let elapsed = start_time.elapsed();
                let remaining = Duration::from_secs_f64(
                    elapsed.as_secs_f64() / finished_runs as f64 * (total_runs - finished_runs) as f64,
                );
                print!("\r{}", " ".repeat(70));
                print!(
                    "\rFinished {}/{} [{}%] runs in {:.2?}, remaining time: {:.2?}",
                    finished_runs,
                    total_runs,
                    (finished_runs as f64 * 100. / total_runs as f64).round() as i32,
                    elapsed,
                    remaining
                );
                std::io::stdout().flush().unwrap();
            });
        }

        pool.join();

        print!("\r{}", " ".repeat(70));
        println!("\rFinished {} runs in {:.2?}", total_runs, start_time.elapsed());

        let mut results = Arc::try_unwrap(results).unwrap().into_inner().unwrap();
        results.sort_by(|a, b| (&a.dag, &a.system, &a.scheduler).cmp(&(&b.dag, &b.system, &b.scheduler)));
        results
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
