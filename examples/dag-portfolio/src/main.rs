use std::cell::RefCell;
use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

extern crate reqwest;

use clap::Parser;

use sugars::{rc, refcell};

use rand::prelude::*;
use rand_pcg::Pcg64;

use log::log_enabled;
use log::Level::Info;

use env_logger::Builder;

use threadpool::ThreadPool;

use dslab_compute::multicore::*;
use dslab_dag::dag::DAG;
use dslab_dag::dag_simulation::DagSimulation;
use dslab_dag::data_item::DataTransferMode;
use dslab_dag::runner::Config;
use dslab_dag::schedulers::portfolio_scheduler::PortfolioScheduler;
use dslab_network::constant_bandwidth_model::ConstantBandwidthNetwork;

#[derive(Parser, Debug)]
#[clap(about, long_about = None)]
struct Args {
    /// Run only one experiment (algo-dag-platform)
    #[clap(long = "run-one")]
    run_one: Option<String>,

    /// Save trace_log to a file
    #[clap(long = "trace-log")]
    trace_log: bool,

    /// Load result from data/results.txt without running simulation
    #[clap(long = "load-results")]
    load_results: bool,

    /// Number of threads
    #[clap(long, default_value = "8")]
    threads: usize,
}

struct RunResult {
    algo: usize,
    dag: usize,
    platform: usize,
    time: f64,
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
            let filename = format!("{}{}-{}.json", dags_folder, i + 1, name);
            eprintln!("Downloading {} to {}...", name, filename);
            let url = [prefix, file].concat();
            let mut resp = reqwest::blocking::get(url).expect("Request to github failed");
            let mut out = std::fs::File::create(filename).unwrap();
            std::io::copy(&mut resp, &mut out).unwrap();
        }

        eprintln!("All workflows saved to folder data/dags");
    }

    let mut dags: Vec<DAG> = Vec::new();
    let mut rng = Pcg64::seed_from_u64(456);

    let mut filenames = std::fs::read_dir(dags_folder)
        .unwrap()
        .map(|path| path.unwrap().file_name().into_string().unwrap())
        .collect::<Vec<_>>();
    filenames.sort();

    for filename in filenames.into_iter() {
        eprintln!("Loading DAG from {}", filename);
        let mut dag = DAG::from_wfcommons(&format!("{}{}", dags_folder, filename), 1.0e11);

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
        // vec![(32, 100, 100), (32, 200, 300), (32, 300, 200)],
        // vec![(32, 100, 200), (32, 200, 100), (32, 300, 300)],
        // vec![(32, 100, 200), (32, 200, 300), (32, 300, 100)],
        // vec![(32, 100, 300), (32, 200, 200), (32, 300, 100)],
        // vec![(32, 100, 300), (32, 200, 100), (32, 300, 200)],
    ];
    let enable_trace_log = args.trace_log;
    let traces_folder = "data/traces/";

    if enable_trace_log {
        std::fs::create_dir_all(traces_folder).unwrap();
    }

    let run_one = args.run_one.as_ref().map(|s| {
        let mut s = s.split('-');
        (
            s.next().unwrap().parse::<usize>().unwrap(),
            s.next().unwrap().parse::<usize>().unwrap(),
            s.next().unwrap().parse::<usize>().unwrap(),
        )
    });

    let pool = ThreadPool::new(args.threads);

    let algos = 36;

    let total_runs = algos * dags.len() * platform_configs.len();
    let finished_runs = Arc::new(AtomicUsize::new(0));

    let results = Arc::new(Mutex::new(Vec::<(usize, usize, usize, f64)>::new()));

    for algo in 0..algos {
        for dag_id in 0..dags.len() {
            for platform_id in 0..platform_configs.len() {
                if let Some((one_algo, one_dag, one_platform)) = run_one {
                    if algo != one_algo || dag_id != one_dag || one_platform != platform_id {
                        continue;
                    }
                }
                let dag = dags[dag_id].clone();
                let platform_config = platform_configs[platform_id].clone();
                let algo = algo;
                let total_runs = total_runs;
                let finished_runs = finished_runs.clone();
                let dag_id = dag_id.clone();
                let platform_id = platform_id.clone();
                let results = results.clone();
                pool.execute(move || {
                    let network_model = Rc::new(RefCell::new(ConstantBandwidthNetwork::new(1.0e+8, 0.)));

                    let scheduler = PortfolioScheduler::new(algo);

                    let mut sim = DagSimulation::new(
                        123,
                        network_model,
                        rc!(refcell!(scheduler)),
                        Config {
                            data_transfer_mode: DataTransferMode::Direct,
                        },
                    );
                    let mut create_resource = |speed: i32, cluster: usize, node: usize| {
                        let name = format!("compute-{}-{}", cluster, node);
                        let speed = speed as u64 * 1_000_000_000 as u64;
                        let cores = 8;
                        let memory = 0;
                        sim.add_resource(&name, speed, cores, memory);
                    };

                    for (cluster, &(nodes, speed, _bandwidth)) in platform_config.iter().enumerate() {
                        for node in 0..nodes {
                            create_resource(speed, cluster, node);
                        }
                    }

                    let runner = sim.init(dag);
                    runner.borrow_mut().enable_trace_log(enable_trace_log);

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

                    finished_runs.fetch_add(1, Ordering::SeqCst);
                    print!(
                        "\rFinished {}/{} runs",
                        finished_runs.load(Ordering::SeqCst),
                        total_runs
                    );
                    std::io::stdout().flush().unwrap();
                    results.lock().unwrap().push((algo, dag_id, platform_id, sim.time()));
                });
            }
        }
    }
    let t = Instant::now();
    pool.join();

    if run_one.is_none() {
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
    } else {
        println!("\rFinished one run in {:.2?}", t.elapsed());
    }
}

fn process_results() {
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
            results.sort_by(|a, b| a.1.total_cmp(&b.1));
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
    algos.sort_by(|&a, &b| avg_ratio_to_best[a].total_cmp(&avg_ratio_to_best[b]));
    for i in 0..algos.len() {
        let algo_ind = *algo_ind.get(&algos[i]).unwrap();
        println!(
            "| {: >4} | {: >13.3} | {: >21.3} | {: >22.3} |",
            algos[i], avg_place[algo_ind], avg_ratio_to_best[algo_ind], first_places_cnt[algo_ind],
        );
    }
}

fn main() {
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    let args = Args::parse();

    if !args.load_results {
        run_experiments(&args);
    }

    if args.run_one.is_some() {
        return;
    }

    process_results();
}
