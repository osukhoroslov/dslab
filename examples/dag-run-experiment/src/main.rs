use std::cell::RefCell;
use std::io::Write;
use std::path::Path;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use serde::{Deserialize, Serialize};

use clap::Parser;

use itertools::Itertools;

use threadpool::ThreadPool;

use dslab_dag::dag::DAG;
use dslab_dag::dag_simulation::DagSimulation;
use dslab_dag::data_item::DataTransferMode;
use dslab_dag::network::read_network;
use dslab_dag::resource::read_resources;
use dslab_dag::runner::Config;
use dslab_dag::scheduler::Scheduler;
use dslab_dag::schedulers::heft::HeftScheduler;
use dslab_dag::schedulers::simple_scheduler::SimpleScheduler;

#[derive(Parser, Debug)]
#[clap(about, long_about = None)]
struct Args {
    /// File with configurations
    #[clap(short, long)]
    input: String,

    /// Output file
    #[clap(short, long)]
    output: Option<String>,

    /// Number of threads for running experiment
    #[clap(short, long, default_value = "8")]
    threads: usize,
}

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

#[derive(Serialize)]
struct RunResult {
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

fn main() {
    let args = Args::parse();

    let config: ExperimentConfig = std::fs::read_to_string(&args.input)
        .ok()
        .and_then(|f| serde_yaml::from_str(&f).ok())
        .expect(&format!("Can't read config from file {}", args.input));

    let dags = get_all_files(&config.dags)
        .into_iter()
        .map(|path| {
            (
                Path::new(&path).file_name().unwrap().to_str().unwrap().to_string(),
                DAG::from_file(&path),
            )
        })
        .collect::<Vec<_>>();

    let systems = get_all_files(&config.systems)
        .into_iter()
        .map(|path| {
            (
                Path::new(&path).file_name().unwrap().to_str().unwrap().to_string(),
                path,
            )
        })
        .map(|(file_name, path)| (read_resources(&path), read_network(&path), file_name))
        .filter(|(resources, network, _file_name)| !resources.is_empty() && network.make_network().is_some())
        .collect::<Vec<_>>();

    let schedulers = config.schedulers;

    eprintln!(
        "Found {} dags, {} systems, {} schedulers",
        dags.len(),
        systems.len(),
        schedulers.len()
    );

    let experiments = dags
        .into_iter()
        .cartesian_product(systems.into_iter())
        .cartesian_product(schedulers.into_iter())
        .map(|((dag, (resources, network, file_name)), scheduler)| (dag, (file_name, resources), network, scheduler))
        .collect::<Vec<_>>();
    let total_runs = experiments.len();
    eprintln!("Total {} experiments", total_runs);

    let finished_runs = Arc::new(AtomicUsize::new(0));
    let result = Arc::new(Mutex::new(Vec::new()));

    let pool = ThreadPool::new(args.threads);
    for ((dag_name, dag), (system_name, resources), network, scheduler_type) in experiments.into_iter() {
        let finished_runs = finished_runs.clone();
        let result = result.clone();
        pool.execute(move || {
            let network = network.make_network().unwrap();
            let scheduler = scheduler_type.make_scheduler();

            let mut sim = DagSimulation::new(
                123,
                network,
                scheduler,
                Config {
                    data_transfer_mode: DataTransferMode::ViaMasterNode,
                },
            );
            for resource in resources.into_iter() {
                sim.add_resource(&resource.name, resource.speed, resource.cores, resource.memory);
            }

            sim.init(dag);
            sim.step_until_no_events();

            let makespan = sim.time();

            result.lock().unwrap().push(RunResult {
                dag: dag_name,
                system: system_name,
                scheduler: format!("{:?}", scheduler_type),
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

    std::fs::File::create(args.output.unwrap_or_else(|| {
        let input = Path::new(&args.input);
        input
            .with_file_name([input.file_stem().unwrap().to_str().unwrap(), "-results"].concat())
            .with_extension("json")
            .to_str()
            .unwrap()
            .to_string()
    }))
    .unwrap()
    .write_all(
        serde_json::to_string_pretty(&*result.lock().unwrap())
            .unwrap()
            .as_bytes(),
    )
    .unwrap();
}
