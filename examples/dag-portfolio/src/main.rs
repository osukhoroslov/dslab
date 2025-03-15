use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use std::str::FromStr;

extern crate reqwest;

use clap::Parser;
use env_logger::Builder;
use rand::prelude::*;
use rand_pcg::Pcg64;

use dslab_compute::multicore::*;
use dslab_dag::dag::DAG;
use dslab_dag::data_item::DataTransferMode;
use dslab_dag::experiment::{Experiment, RunResult};
use dslab_dag::network::NetworkConfig;
use dslab_dag::parsers::config::ParserConfig;
use dslab_dag::resource::ResourceConfig;
use dslab_dag::scheduler::{default_scheduler_resolver, SchedulerParams};
use dslab_dag::schedulers::dynamic_list::{CoresCriterion, DynamicListStrategy, ResourceCriterion, TaskCriterion};

const TASK_CRITERIA: &[TaskCriterion] = &[
    TaskCriterion::CompSize,
    TaskCriterion::DataSize,
    TaskCriterion::ChildrenCount,
    TaskCriterion::BottomLevel,
];

const RESOURCE_CRITERIA: &[ResourceCriterion] = &[
    ResourceCriterion::Speed,
    ResourceCriterion::TaskData,
    ResourceCriterion::MaxAvailableCores,
];

const CORES_CRITERIA: &[CoresCriterion] = &[
    CoresCriterion::MaxCores,
    CoresCriterion::Efficiency90,
    CoresCriterion::Efficiency50,
];

#[derive(Parser, Debug)]
#[command(about, long_about = None)]
struct Args {
    /// Save trace logs to data/traces/
    #[arg(long)]
    save_traces: bool,

    /// Load results from data/results.json without running simulation
    #[arg(long)]
    load_results: bool,

    ///  Number of threads to use (default - use all available cores)
    #[arg(long, default_value_t = std::thread::available_parallelism().unwrap().get())]
    threads: usize,
}

fn main() {
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();
    let args = Args::parse();
    if !args.load_results {
        run_experiments(&args);
    }
    process_results();
}

fn run_experiments(args: &Args) {
    let dags_folder = "data/dags/";
    if !Path::new(dags_folder).exists() {
        eprint!("No directory with workflows found. Download them from github? [y/n] ");
        let mut buffer = String::new();
        std::io::stdin().read_line(&mut buffer).unwrap();
        if buffer.trim() != "y" {
            return;
        }

        std::fs::create_dir_all(dags_folder).unwrap();

        let prefix = "https://raw.githubusercontent.com/wrench-project/jsspp2022_submission_data/main/workflows/";
        let workflows = vec![
            ("1000Genome", "1000genome-chameleon-8ch-250k-001.json"),
            ("Blast", "blast-chameleon-medium-002.json"),
            ("Bwa", "bwa-chameleon-large-003.json"),
            ("Cycles", "cycles-chameleon-2l-2c-12p-001.json"),
            ("Epigenomics", "epigenomics-chameleon-ilmn-4seq-50k-001.json"),
            ("Montage", "montage-chameleon-2mass-10d-001.json"),
            ("Soykb", "soykb-chameleon-10fastq-20ch-001.json"),
            ("Srasearch", "srasearch-chameleon-10a-003.json"),
        ];

        for (i, (name, file)) in workflows.into_iter().enumerate() {
            let filename = format!("{}{}-{}.json", dags_folder, i + 1, name);
            eprintln!("Downloading {} to {}...", name, filename);
            let url = [prefix, file].concat();
            let mut resp = reqwest::blocking::get(url).expect("Request to github failed");
            let mut out = std::fs::File::create(filename).unwrap();
            std::io::copy(&mut resp, &mut out).unwrap();
        }

        eprintln!("All workflows saved to folder data/dags");
    }

    let mut dags: Vec<(String, DAG)> = Vec::new();
    let mut rng = Pcg64::seed_from_u64(456);

    let mut filenames = std::fs::read_dir(dags_folder)
        .unwrap()
        .map(|path| path.unwrap().file_name().into_string().unwrap())
        .collect::<Vec<_>>();
    filenames.sort();

    for filename in filenames.into_iter() {
        eprintln!("Loading DAG from {}", filename);
        let mut dag = DAG::from_wfcommons(
            format!("{}{}", dags_folder, filename),
            &ParserConfig {
                reference_speed: 10.,
                ignore_memory: true,
                ..ParserConfig::default()
            },
        );

        for task_id in 0..dag.get_tasks().len() {
            let task = dag.get_task_mut(task_id);
            task.max_cores = 100; // no limit
            task.cores_dependency = CoresDependency::LinearWithFixed {
                fixed_part: rng.gen_range(0.0..0.2),
            }
        }
        dags.push((filename.replace(".json", ""), dag));
    }

    // up to 3 clusters for each platform, each triple means (nodes, speed in Gflop/s, bandwidth in MB/s)
    // currently different bandwidth for different clusters is not supported
    let platform_configs = [
        vec![(96, 10., 100.)],
        vec![(48, 5., 100.), (48, 15., 100.)],
        vec![(48, 5., 100.), (48, 40., 10.)],
        vec![(32, 10., 100.), (32, 20., 200.), (32, 30., 300.)],
        // vec![(32, 10., 100.), (32, 20., 300.), (32, 30., 200.)],
        // vec![(32, 10., 200.), (32, 20., 100.), (32, 30., 300.)],
        // vec![(32, 10., 200.), (32, 20., 300.), (32, 30., 100.)],
        // vec![(32, 10., 300.), (32, 20., 200.), (32, 30., 100.)],
        // vec![(32, 10., 300.), (32, 20., 100.), (32, 30., 200.)],
    ];
    let mut systems = Vec::new();
    for (id, platform_config) in platform_configs.iter().enumerate() {
        let system_name = format!("system-{id}");
        let mut resources = Vec::new();
        for (cluster, &(nodes, speed, _bandwidth)) in platform_config.iter().enumerate() {
            for node in 0..nodes {
                let name = format!("compute-{}-{}", cluster, node);
                let cores = 8;
                let memory = 0; // memory usage is ignored
                resources.push(ResourceConfig {
                    name,
                    speed,
                    cores,
                    memory,
                    price: 0.,
                });
            }
        }
        let network_config = NetworkConfig::constant(1000., 0.);
        systems.push((system_name, resources, network_config));
    }

    let mut algos = Vec::new();
    for task_criterion in TASK_CRITERIA {
        for resource_criterion in RESOURCE_CRITERIA {
            for cores_criterion in CORES_CRITERIA {
                let algo_str = format!(
                    "DynamicList[task={},resource={},cores={}]",
                    task_criterion, resource_criterion, cores_criterion
                );
                algos.push(SchedulerParams::from_str(&algo_str).unwrap());
            }
        }
    }

    let traces_dir = args.save_traces.then(|| "data/traces".to_string());

    let experiment = Experiment::new(
        dags,
        systems,
        DataTransferMode::Direct,
        algos,
        default_scheduler_resolver,
        traces_dir,
        None,
    );

    let mut results = experiment.run(args.threads);

    // we replace exec_time with 0 to avoid changes of results file (stored in git)
    // due to simulation speed differences
    results.iter_mut().for_each(|result| result.exec_time = 0.);
    std::fs::File::create("data/results.json")
        .unwrap()
        .write_all(serde_json::to_string_pretty(&results).unwrap().as_bytes())
        .unwrap();
}

fn process_results() {
    let results: Vec<RunResult> = serde_json::from_str(&std::fs::read_to_string("data/results.json").unwrap()).unwrap();
    let mut grouped_results: HashMap<(String, String), HashMap<String, f64>> = HashMap::new();
    for result in results {
        grouped_results
            .entry((result.dag, result.system))
            .or_default()
            .insert(result.scheduler, result.makespan);
    }

    let mut avg_ratio_to_best = HashMap::new();
    let mut first_places_cnt = HashMap::new();
    for algo_results in grouped_results.values() {
        let best_makespan = algo_results.values().min_by(|a, b| a.total_cmp(b)).unwrap();
        for (algo, makespan) in algo_results.iter() {
            *avg_ratio_to_best.entry(algo.clone()).or_insert(0.) += makespan / best_makespan;
            if makespan == best_makespan {
                *first_places_cnt.entry(algo.clone()).or_insert(0) += 1;
            }
        }
    }
    for (_, val) in avg_ratio_to_best.iter_mut() {
        *val /= grouped_results.len() as f64;
    }
    let mut avg_ratio_to_best = Vec::from_iter(avg_ratio_to_best);
    avg_ratio_to_best.sort_by(|&(_, a), &(_, b)| a.total_cmp(&b));

    println!("| task crit          | resource crit      | cores crit         | avg ratio | # best |");
    println!("|--------------------|--------------------|--------------------|-----------|--------|");
    for (algo, avg_ratio) in avg_ratio_to_best.iter() {
        let strategy = DynamicListStrategy::from_params(&SchedulerParams::from_str(algo).unwrap());
        println!(
            "| {: >18} | {: >18} | {: >18} | {: >9.3} | {: >6.3} |",
            strategy.task_criterion,
            strategy.resource_criterion,
            strategy.cores_criterion,
            avg_ratio,
            first_places_cnt.remove(algo).unwrap_or(0)
        );
    }
}
