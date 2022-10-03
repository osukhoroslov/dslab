use std::cell::RefCell;
use std::io::Write;
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
use dslab_dag::network::read_network;
use dslab_dag::resource::read_resources;
use dslab_dag::runner::{Config, DataTransferMode};
use dslab_dag::scheduler::Scheduler;
use dslab_dag::schedulers::heft::HeftScheduler;
use dslab_dag::schedulers::simple_scheduler::SimpleScheduler;

#[derive(Parser, Debug)]
#[clap(about, long_about = None)]
struct Args {
    /// Folder with DAGs
    #[clap(long)]
    dags: String,

    /// Folder with system configurations (resources + network)
    #[clap(long)]
    systems: String,

    /// File with schedulers configuration
    #[clap(long)]
    schedulers: String,

    /// Output file
    #[clap(short, long)]
    output: String,

    /// Number of parallel jobs
    #[clap(short, long, default_value = "8")]
    jobs: usize,
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
struct ExperimentResult {
    dag: String,
    system: String,
    scheduler: String,
    makespan: f64,
}

fn main() {
    let args = Args::parse();

    let dags = std::fs::read_dir(args.dags).expect("Can't open directory with dags");
    let dags = dags
        .filter_map(|x| x.ok())
        .filter(|path| path.path().is_file())
        .map(|path| {
            (
                path.file_name().to_str().unwrap().to_string(),
                match path.file_name().to_str().unwrap().split('.').last().unwrap() {
                    "yaml" => DAG::from_yaml(path.path().to_str().unwrap()),
                    "xml" => DAG::from_dax(path.path().to_str().unwrap(), 1000.),
                    "dot" => DAG::from_dot(path.path().to_str().unwrap()),
                    x => {
                        eprintln!("Wrong file format for dag: {}", x);
                        std::process::exit(1);
                    }
                },
            )
        })
        .collect::<Vec<_>>();

    let systems = std::fs::read_dir(args.systems).expect("Can't open directory with systems");
    let systems = systems
        .filter_map(|x| x.ok())
        .filter(|path| path.path().is_file())
        .map(|path| {
            (
                path.file_name().to_str().unwrap().to_string(),
                path.path().to_str().unwrap().to_string(),
            )
        })
        .map(|(file_name, path)| (read_resources(&path), read_network(&path), file_name))
        .filter(|(resources, network, _file_name)| !resources.is_empty() && network.make_network().is_some())
        .collect::<Vec<_>>();

    let schedulers: Vec<YamlScheduler> = serde_yaml::from_str(
        &std::fs::read_to_string(&args.schedulers).expect(&format!("Can't read file {}", &args.schedulers)),
    )
    .expect(&format!("Can't parse YAML from file {}", &args.schedulers));

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

    let pool = ThreadPool::new(args.jobs);
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

            result.lock().unwrap().push(ExperimentResult {
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

    std::fs::File::create(args.output)
        .unwrap()
        .write_all(
            serde_json::to_string_pretty(&*result.lock().unwrap())
                .unwrap()
                .as_bytes(),
        )
        .unwrap();
}
