use std::cell::RefCell;
use std::path::Path;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use serde::{Deserialize, Serialize};

use itertools::Itertools;

use threadpool::ThreadPool;

use crate::dag::DAG;
use crate::dag_simulation::DagSimulation;
use crate::data_item::DataTransferMode;
use crate::network::read_network;
use crate::network::Network;
use crate::resource::read_resources;
use crate::resource::YamlResource;
use crate::runner::Config;
use crate::scheduler::Scheduler;
use crate::schedulers::heft::HeftScheduler;
use crate::schedulers::simple_scheduler::SimpleScheduler;

#[derive(Debug, Deserialize, Clone)]
enum YamlScheduler {
    Simple,
    Heft,
}

impl YamlScheduler {
    fn make_scheduler(&self) -> Rc<RefCell<dyn Scheduler>> {
        match self {
            YamlScheduler::Simple => Rc::new(RefCell::new(SimpleScheduler::new())),
            YamlScheduler::Heft => Rc::new(RefCell::new(HeftScheduler::new())),
        }
    }
}

#[derive(Serialize, Debug)]
pub struct RunResult {
    dag: String,
    system: String,
    scheduler: String,
    makespan: f64,
}

#[derive(Deserialize)]
struct ExperimentConfig {
    dags: Vec<String>,
    systems: Vec<String>,
    schedulers: Vec<YamlScheduler>,
}

fn get_all_files(paths: &[String]) -> Vec<String> {
    let mut result = Vec::new();
    for path in paths.iter() {
        if Path::new(&path).is_dir() {
            result.extend(get_all_files(
                &std::fs::read_dir(path)
                    .unwrap()
                    .map(|s| s.unwrap().path().to_str().unwrap().to_string())
                    .collect::<Vec<String>>(),
            ));
        } else {
            result.push(path.to_string());
        }
    }
    result
}

struct Run {
    dag_name: String,
    dag: DAG,
    resources: Vec<YamlResource>,
    network: Network,
    system_name: String,
    scheduler: YamlScheduler,
}

pub struct Experiment {
    runs: Vec<Run>,
}

impl Experiment {
    pub fn load(file: &str) -> Self {
        let config: ExperimentConfig = std::fs::read_to_string(file)
            .ok()
            .and_then(|f| serde_yaml::from_str(&f).ok())
            .unwrap_or_else(|| panic!("Can't read config from file {}", file));

        let dags = get_all_files(&config.dags).into_iter().map(|path| {
            (
                Path::new(&path).file_name().unwrap().to_str().unwrap().to_string(),
                DAG::from_file(&path),
            )
        });

        let systems = get_all_files(&config.systems)
            .into_iter()
            .map(|path| {
                (
                    Path::new(&path).file_name().unwrap().to_str().unwrap().to_string(),
                    path,
                )
            })
            .map(|(file_name, path)| (read_resources(&path), read_network(&path), file_name))
            .filter(|(resources, network, _file_name)| !resources.is_empty() && network.make_network().is_some());

        let schedulers = config.schedulers;

        let runs = dags
            .cartesian_product(systems)
            .cartesian_product(schedulers.into_iter())
            .map(
                |(((dag_name, dag), (resources, network, system_name)), scheduler)| Run {
                    dag_name,
                    dag,
                    resources,
                    network,
                    system_name,
                    scheduler,
                },
            )
            .collect::<Vec<_>>();

        Self { runs }
    }

    pub fn run(self, threads: usize) -> Vec<RunResult> {
        let total_runs = self.runs.len();

        let finished_runs = Arc::new(AtomicUsize::new(0));
        let result = Arc::new(Mutex::new(Vec::new()));

        let pool = ThreadPool::new(threads);
        for run in self.runs.into_iter() {
            let finished_runs = finished_runs.clone();
            let result = result.clone();
            pool.execute(move || {
                let network = run.network.make_network().unwrap();
                let scheduler = run.scheduler.make_scheduler();

                let mut sim = DagSimulation::new(
                    123,
                    network,
                    scheduler,
                    Config {
                        data_transfer_mode: DataTransferMode::ViaMasterNode,
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
                    scheduler: format!("{:?}", run.scheduler),
                    makespan,
                });

                finished_runs.fetch_add(1, Ordering::SeqCst);
                print!(
                    "\rFinished {}/{} runs",
                    finished_runs.load(Ordering::SeqCst),
                    total_runs
                );
            });
        }

        let t = Instant::now();
        pool.join();

        println!("\rFinished {} runs in {:.2?}", total_runs, t.elapsed());

        Arc::try_unwrap(result).unwrap().into_inner().unwrap()
    }
}
