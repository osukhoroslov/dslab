use std::cell::RefCell;
use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::Instant;

extern crate reqwest;

use clap::{command, Arg, ArgMatches};

use sugars::{rc, refcell};

use rand::prelude::*;
use rand_pcg::Pcg64;

use log::log_enabled;
use log::Level::Info;

use env_logger::Builder;

use threadpool::ThreadPool;

use compute::multicore::*;
use dag::dag::DAG;
use dag::resource::Resource;
use dag::runner::*;
use dag::scheduler::{Config, Scheduler};
use dag::schedulers::portfolio_scheduler::PortfolioScheduler;
use network::constant_bandwidth_model::ConstantBandwidthNetwork;
use network::network::Network;
use simcore::simulation::Simulation;

struct RunResult {
    algo: usize,
    dag: usize,
    platform: usize,
    time: f64,
}

fn run_experiments(matches: &ArgMatches) {
    let graphs_folder = "data/graphs/";
    if !Path::new(graphs_folder).exists() {
        eprint!("No directory with workflows found. Download them from github? [y/n] ");
        let mut buffer = String::new();
        std::io::stdin().read_line(&mut buffer).unwrap();
        if buffer.trim() != "y" {
            return;
        }

        std::fs::create_dir_all(graphs_folder).unwrap();

        let prefix = "https://raw.githubusercontent.com/wrench-project/jsspp2022_submission_data/main/workflows/";
        let workflows = vec![
            ("Montage", "montage-chameleon-2mass-10d-001.json"),
            ("Epigenomics", "epigenomics-chameleon-ilmn-4seq-50k-001.json"),
            ("Bwa", "bwa-chameleon-large-003.json"),
            ("Cycles", "cycles-chameleon-2l-2c-12p-001.json"),
            ("1000Genome", "1000genome-chameleon-8ch-250k-001.json"),
            ("Blast", "blast-chameleon-medium-002.json"),
            ("Soykb", "soykb-chameleon-10fastq-20ch-001.json"),
            ("Srasearch", "srasearch-chameleon-10a-003.json"),
        ];

        for (i, (name, file)) in workflows.into_iter().enumerate() {
            let filename = format!("{}{}-{}.json", graphs_folder, i + 1, name);
            eprintln!("Downloading {} to {}...", name, filename);
            let url = [prefix, file].concat();
            let mut resp = reqwest::blocking::get(url).expect("Request to github failed");
            let mut out = std::fs::File::create(filename).unwrap();
            std::io::copy(&mut resp, &mut out).unwrap();
        }

        eprintln!("All workflows saved to folder data/graphs");
    }

    let mut dags: Vec<DAG> = Vec::new();
    let mut rng = Pcg64::seed_from_u64(456);

    for path in std::fs::read_dir(graphs_folder).unwrap() {
        let filename = path.unwrap().file_name().into_string().unwrap();
        eprintln!("Loading DAG from {}", filename);
        let mut dag = DAG::from_wfcommons(&format!("{}{}", graphs_folder, filename), 1.0e+10);

        for task_id in 0..dag.get_tasks().len() {
            let task = dag.get_task_mut(task_id);
            task.max_cores = 100; // no limit
            task.cores_dependency = CoresDependency::LinearWithFixed {
                fixed_part: rng.gen_range(0.0..0.2),
            }
        }
        dags.push(dag);
    }

    // up to 3 clusters for each platform, each triple means (nodes, speed, bandwidth)
    // currently different bandwidth for different clusters is not supported
    let platform_configs = vec![
        vec![(96, 100, 100)],
        vec![(48, 50, 100), (48, 150, 100)],
        vec![(48, 50, 100), (48, 400, 10)],
        vec![(32, 100, 100), (32, 200, 200), (32, 300, 300)],
        vec![(32, 100, 100), (32, 200, 300), (32, 300, 200)],
        vec![(32, 100, 200), (32, 200, 100), (32, 300, 300)],
        vec![(32, 100, 200), (32, 200, 300), (32, 300, 100)],
        vec![(32, 100, 300), (32, 200, 200), (32, 300, 100)],
        vec![(32, 100, 300), (32, 200, 100), (32, 300, 200)],
    ];
    let enable_trace_log = matches.is_present("trace-log");
    let traces_folder = "data/traces/";

    if enable_trace_log {
        std::fs::create_dir_all(traces_folder).unwrap();
    }

    let num_threads: usize = matches.value_of_t("threads").unwrap();
    let pool = ThreadPool::new(num_threads);

    // one criterion picks cluster based on the data already available there,
    // such algorithms are skipped since direct data transfer between nodes is not supported yet
    let algos = (0..36).filter(|&algo| algo % 9 / 3 != 0).collect::<Vec<_>>();

    let total_runs = algos.len() * dags.len() * platform_configs.len();
    let finished_runs = Arc::new(Mutex::new(0));

    let results = Arc::new(Mutex::new(Vec::<(i32, usize, usize, f64)>::new()));

    for algo in algos.into_iter() {
        for dag_id in 0..dags.len() {
            for platform_id in 0..platform_configs.len() {
                let dag = dags[dag_id].clone();
                let platform_config = platform_configs[platform_id].clone();
                let algo = algo;
                let total_runs = total_runs;
                let finished_runs = finished_runs.clone();
                let dag_id = dag_id.clone();
                let platform_id = platform_id.clone();
                let results = results.clone();
                pool.execute(move || {
                    let mut sim = Simulation::new(123);

                    let mut create_resource = |speed: i32, cluster: usize, node: usize| -> Resource {
                        let name = format!("compute-{}-{}", cluster, node);
                        let speed = speed as u64 * 1_000_000_000 as u64;
                        let cores = 8;
                        let memory = 0;
                        let compute = Rc::new(RefCell::new(Compute::new(
                            speed,
                            cores,
                            memory,
                            sim.create_context(&name),
                        )));
                        let id = sim.add_handler(&name, compute.clone());
                        Resource {
                            id,
                            name,
                            compute,
                            speed,
                            cores_available: cores,
                            memory_available: memory,
                        }
                    };

                    let mut resources: Vec<Resource> = Vec::new();
                    for (cluster, &(nodes, speed, _bandwidth)) in platform_config.iter().enumerate() {
                        for node in 0..nodes {
                            resources.push(create_resource(speed, cluster, node));
                        }
                    }

                    let network_model = Rc::new(RefCell::new(ConstantBandwidthNetwork::new(1.0e+8, 0.)));

                    let mut scheduler = PortfolioScheduler::new(algo);
                    scheduler.set_config(Config {
                        network: network_model.clone(),
                    });

                    let network = rc!(refcell!(Network::new(network_model.clone(), sim.create_context("net"))));
                    sim.add_handler("net", network.clone());

                    let runner = rc!(refcell!(DAGRunner::new(
                        dag,
                        network,
                        resources,
                        Rc::new(RefCell::new(scheduler)),
                        sim.create_context("runner")
                    )
                    .enable_trace_log(enable_trace_log)));
                    let runner_id = sim.add_handler("runner", runner.clone());

                    let mut client = sim.create_context("client");
                    client.emit_now(Start {}, runner_id);

                    let t = Instant::now();
                    sim.step_until_no_events();
                    if log_enabled!(Info) {
                        println!(
                            "Processed {} events in {:.2?} ({:.0} events/sec)",
                            sim.event_count(),
                            t.elapsed(),
                            sim.event_count() as f64 / t.elapsed().as_secs_f64()
                        );
                    }
                    runner.borrow().validate_completed();
                    if enable_trace_log {
                        runner
                            .borrow()
                            .trace_log()
                            .save_to_file(&format!(
                                "{}{:0>3}-{:0>3}-{:0>3}.json",
                                traces_folder, algo, dag_id, platform_id
                            ))
                            .unwrap();
                    }

                    let mut finished_runs = finished_runs.lock().unwrap();
                    *finished_runs += 1;
                    print!("\rFinished {}/{} runs", finished_runs, total_runs);
                    std::io::stdout().flush().unwrap();
                    results.lock().unwrap().push((algo, dag_id, platform_id, sim.time()));
                });
            }
        }
    }
    let t = Instant::now();
    pool.join();
    println!("\rFinished {} runs in {:.2?}", total_runs, t.elapsed());

    let mut results = results.lock().unwrap();
    results.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)).then(a.2.cmp(&b.2)));

    std::fs::File::create("data/results.txt")
        .unwrap()
        .write_all(
            results
                .iter()
                .map(|(algo, dag_id, platform_id, time)| {
                    format!("{}\t{}\t{}\t{:.10}\n", algo, dag_id, platform_id, time)
                })
                .collect::<Vec<_>>()
                .join("")
                .as_bytes(),
        )
        .unwrap();
}

fn main() {
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    let matches = command!()
        .arg(
            Arg::new("trace-log")
                .long("trace-log")
                .help("Save trace_log to file")
                .takes_value(false),
        )
        .arg(
            Arg::new("load-results")
                .long("load-results")
                .help("Load result from data/results.txt without running simulation")
                .takes_value(false),
        )
        .arg(
            Arg::new("threads")
                .long("threads")
                .help("Number of threads")
                .default_value("8"),
        )
        .get_matches();

    let load_results = matches.is_present("load-results");

    if !load_results {
        run_experiments(&matches);
    }

    let results = std::fs::read_to_string("data/results.txt")
        .unwrap()
        .split('\n')
        .filter(|s| !s.is_empty())
        .map(|s| {
            let mut s = s.split('\t');
            RunResult {
                algo: s.next().unwrap().parse::<usize>().unwrap(),
                dag: s.next().unwrap().parse::<usize>().unwrap(),
                platform: s.next().unwrap().parse::<usize>().unwrap(),
                time: s.next().unwrap().parse::<f64>().unwrap(),
            }
        })
        .collect::<Vec<_>>();

    let mut algos = results.iter().map(|t| t.algo).collect::<Vec<_>>();
    algos.sort();
    algos.dedup();
    let algo_ind: HashMap<usize, usize> = algos.iter().enumerate().map(|(x, &y)| (y, x)).collect();

    let dags = results.iter().map(|t| t.dag).max().unwrap() + 1;
    let platforms = results.iter().map(|t| t.platform).max().unwrap() + 1;

    let mut data = vec![vec![vec![0.; platforms]; dags]; algos.len()];
    for result in results.into_iter() {
        data[*algo_ind.get(&result.algo).unwrap()][result.dag][result.platform] = result.time;
    }

    let mut avg_place = vec![0.; algos.len()];
    let mut avg_ratio_to_best = vec![0.; algos.len()];
    let mut first_places_cnt = vec![0; algos.len()];

    for dag in 0..dags {
        for platform in 0..platforms {
            let mut results: Vec<(usize, f64)> =
                (0..algos.len()).map(|algo| (algo, data[algo][dag][platform])).collect();
            results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            let best_time = results[0].1;
            first_places_cnt[results[0].0] += 1;
            for (i, (algo, tm)) in results.into_iter().enumerate() {
                avg_place[algo] += (i + 1) as f64;
                avg_ratio_to_best[algo] += tm / best_time;
            }
        }
    }

    let total_runs = dags * platforms;
    for i in 0..algos.len() {
        avg_place[i] /= total_runs as f64;
        avg_ratio_to_best[i] /= total_runs as f64;
    }

    println!("| algo | average place | average ratio to best | number of first places |");
    println!("|------|---------------|-----------------------|------------------------|");
    for i in 0..algos.len() {
        println!(
            "| {: >4} | {: >13.3} | {: >21.3} | {: >22.3} |",
            algos[i], avg_place[i], avg_ratio_to_best[i], first_places_cnt[i],
        );
    }
}
