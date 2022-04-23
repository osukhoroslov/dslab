mod schedulers;

use std::cell::RefCell;
use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::Instant;

extern crate reqwest;

use clap::{command, Arg, ArgEnum};

use sugars::{rc, refcell};

use rand::prelude::*;
use rand_pcg::Pcg64;

use log::log_enabled;
use log::Level::Info;

use env_logger::Builder;

use threadpool::ThreadPool;

use compute::multicore::*;
use dag::dag::DAG;
use dag::network::load_network;
use dag::resource::{load_resources, Resource};
use dag::runner::*;
use dag::scheduler::{Config, Scheduler};
use network::constant_bandwidth_model::ConstantBandwidthNetwork;
use network::network::Network;
use simcore::simulation::Simulation;

use crate::schedulers::heft::{DataTransferMode, DataTransferStrategy, HeftScheduler};
use crate::schedulers::portfolio_scheduler::PortfolioScheduler;
use crate::schedulers::simple_scheduler::SimpleScheduler;

#[derive(ArgEnum, Clone, Debug)]
pub enum ArgScheduler {
    Simple,
    Heft,
}

fn run_simulation(dag: DAG, resources_file: &str, network_file: &str, trace_file: &str) {
    let mut sim = Simulation::new(123);

    let resources = load_resources(resources_file, &mut sim);

    let network_model = load_network(network_file);

    let matches = command!()
        .arg(
            Arg::new("trace-log")
                .long("trace-log")
                .help("Save trace_log to file")
                .takes_value(false),
        )
        .arg(
            Arg::new("scheduler")
                .long("scheduler")
                .help(
                    format!(
                        "Scheduler {}",
                        format!("{:?}", ArgScheduler::value_variants()).to_lowercase()
                    )
                    .as_str(),
                )
                .validator(|s| ArgScheduler::from_str(s, true))
                .default_value("heft")
                .takes_value(true),
        )
        .get_matches();

    let enable_trace_log = matches.is_present("trace-log");
    let scheduler: Rc<RefCell<dyn Scheduler>> =
        match ArgScheduler::from_str(matches.value_of("scheduler").unwrap(), true).unwrap() {
            ArgScheduler::Simple => rc!(refcell!(SimpleScheduler::new())),
            ArgScheduler::Heft => {
                rc!(refcell!(HeftScheduler::new()
                    .with_data_transfer_mode(DataTransferMode::ViaMasterNode)
                    .with_data_transfer_strategy(DataTransferStrategy::Lazy)))
            }
        };
    scheduler.borrow_mut().set_config(Config {
        network: network_model.clone(),
    });

    let network = rc!(refcell!(Network::new(network_model, sim.create_context("net"))));
    sim.add_handler("net", network.clone());

    let runner = rc!(refcell!(DAGRunner::new(
        dag,
        network,
        resources,
        scheduler,
        sim.create_context("runner")
    )
    .enable_trace_log(enable_trace_log)));
    let runner_id = sim.add_handler("runner", runner.clone());

    let mut client = sim.create_context("client");
    client.emit_now(Start {}, runner_id);

    let t = Instant::now();
    sim.step_until_no_events();
    println!(
        "Processed {} events in {:.2?} ({:.0} events/sec)",
        sim.event_count(),
        t.elapsed(),
        sim.event_count() as f64 / t.elapsed().as_secs_f64()
    );
    runner.borrow().validate_completed();
    if enable_trace_log {
        runner.borrow().trace_log().save_to_file(trace_file).unwrap();
    }

    println!();
}

fn map_reduce() {
    let mut dag = DAG::new();

    let data_part1 = dag.add_data_item("part1", 128);
    let data_part2 = dag.add_data_item("part2", 64);

    let map1 = dag.add_task("map1", 100, 512, 1, 2, CoresDependency::Linear);
    dag.add_data_dependency(data_part1, map1);
    let map1_out1 = dag.add_task_output(map1, "map1_out1", 10);
    let map1_out2 = dag.add_task_output(map1, "map1_out2", 10);
    let map1_out3 = dag.add_task_output(map1, "map1_out3", 10);
    let map1_out4 = dag.add_task_output(map1, "map1_out4", 10);

    let map2 = dag.add_task("map2", 120, 512, 2, 4, CoresDependency::Linear);
    dag.add_data_dependency(data_part2, map2);
    let map2_out1 = dag.add_task_output(map2, "map2_out1", 10);
    let map2_out2 = dag.add_task_output(map2, "map2_out2", 10);
    let map2_out3 = dag.add_task_output(map2, "map2_out3", 10);
    let map2_out4 = dag.add_task_output(map2, "map2_out4", 10);

    let reduce1 = dag.add_task("reduce1", 60, 128, 2, 3, CoresDependency::Linear);
    dag.add_data_dependency(map1_out1, reduce1);
    dag.add_data_dependency(map2_out1, reduce1);

    let reduce2 = dag.add_task("reduce2", 50, 128, 1, 1, CoresDependency::Linear);
    dag.add_data_dependency(map1_out2, reduce2);
    dag.add_data_dependency(map2_out2, reduce2);

    let reduce3 = dag.add_task("reduce3", 100, 128, 1, 2, CoresDependency::Linear);
    dag.add_data_dependency(map1_out3, reduce3);
    dag.add_data_dependency(map2_out3, reduce3);

    let reduce4 = dag.add_task("reduce4", 110, 128, 1, 1, CoresDependency::Linear);
    dag.add_data_dependency(map1_out4, reduce4);
    dag.add_data_dependency(map2_out4, reduce4);

    dag.add_task_output(reduce1, "result1", 32);
    dag.add_task_output(reduce2, "result2", 32);
    dag.add_task_output(reduce3, "result3", 32);
    dag.add_task_output(reduce4, "result4", 32);

    run_simulation(
        dag,
        "resources/cluster1.yaml",
        "networks/network1.yaml",
        "traces/trace_map_reduce.json",
    );
}

fn epigenomics() {
    run_simulation(
        DAG::from_dax("graphs/Epigenomics_100.xml", 1000.),
        "resources/cluster2.yaml",
        "networks/network2.yaml",
        "traces/trace_epigenomics.json",
    );
}

fn montage() {
    run_simulation(
        DAG::from_dot("graphs/Montage.dot"),
        "resources/cluster2.yaml",
        "networks/network3.yaml",
        "traces/trace_montage.json",
    );
}

fn diamond() {
    run_simulation(
        DAG::from_yaml("graphs/diamond.yaml"),
        "resources/cluster3.yaml",
        "networks/network4.yaml",
        "traces/trace_diamond.json",
    );
}

fn reuse_files() {
    let mut dag = DAG::new();

    let input = dag.add_data_item("input", 128);

    let mut rng = Pcg64::seed_from_u64(456);

    let a_cnt = 10;
    let b_cnt = 10;
    let deps_cnt = 3;

    for i in 0..a_cnt {
        let task = dag.add_task(&format!("a{}", i), 100, 128, 1, 2, CoresDependency::Linear);
        dag.add_data_dependency(input, task);
        dag.add_task_output(task, &format!("a{}_out", i), 10);
    }

    for i in 0..b_cnt {
        let task = dag.add_task(&format!("b{}", i), 100, 128, 1, 2, CoresDependency::Linear);
        let mut deps = (0..deps_cnt).map(|_| rng.gen_range(0..a_cnt) + 1).collect::<Vec<_>>();
        deps.sort();
        deps.dedup();
        for dep in deps.into_iter() {
            dag.add_data_dependency(dep, task);
        }
        dag.add_task_output(task, &format!("b{}_out", i), 10);
    }

    run_simulation(
        dag,
        "resources/cluster1.yaml",
        "networks/network1.yaml",
        "traces/trace_reuse_files.json",
    );
}

fn portfolio36() {
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
                .help("Load result from portfolio36/results.txt without running simulation")
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
        let graphs_folder = "portfolio36/graphs/";
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

            eprintln!("All workflows saved to folder portfolio36/graphs");
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

        // up to 3 clusters for each platrform, each triple means (nodes, speed, bandwidth)
        // currently different bandwidth for different clusters is not supported
        let platforms_config = vec![
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
        let traces_folder = "portfolio36/traces/";

        if enable_trace_log {
            std::fs::create_dir_all(traces_folder).unwrap();
        }

        let num_threads: usize = matches.value_of_t("threads").unwrap();
        let pool = ThreadPool::new(num_threads);

        // one criterion picks cluster based on the data already available there, but it's useless in current state
        let algos = (0..36).filter(|&algo| algo % 9 / 3 != 0).collect::<Vec<_>>();

        let total_runs = algos.len() * dags.len() * platforms_config.len();
        let finished_runs = Arc::new(Mutex::new(0));

        let results = Arc::new(Mutex::new(Vec::<(i32, usize, usize, f64)>::new()));

        for algo in algos.into_iter() {
            for dag_id in 0..dags.len() {
                for platform_id in 0..platforms_config.len() {
                    let dag = dags[dag_id].clone();
                    let platform_config = platforms_config[platform_id].clone();
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

        std::fs::File::create("portfolio36/results.txt")
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

    let results = std::fs::read_to_string("portfolio36/results.txt")
        .unwrap()
        .split('\n')
        .filter(|s| !s.is_empty())
        .map(|s| {
            let mut s = s.split('\t');
            (
                s.next().unwrap().parse::<usize>().unwrap(),
                s.next().unwrap().parse::<usize>().unwrap(),
                s.next().unwrap().parse::<usize>().unwrap(),
                s.next().unwrap().parse::<f64>().unwrap(),
            )
        })
        .collect::<Vec<_>>();

    let mut algos = results.iter().map(|t| t.0).collect::<Vec<_>>();
    algos.sort();
    algos.dedup();
    let algo_ind: HashMap<usize, usize> = algos.iter().enumerate().map(|(x, &y)| (y, x)).collect();

    let dags = results.iter().map(|t| t.1).max().unwrap() + 1;
    let platforms = results.iter().map(|t| t.2).max().unwrap() + 1;

    let mut data = vec![vec![vec![0.; platforms]; dags]; algos.len()];
    for (algo, dag, platform, time) in results.into_iter() {
        data[*algo_ind.get(&algo).unwrap()][dag][platform] = time;
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

fn main() {
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    let run_examples = false;
    let run_portfolio36 = true;

    if run_examples {
        map_reduce();
        epigenomics(); // dax
        montage(); // dot
        diamond(); // yaml
        reuse_files();
    }

    if run_portfolio36 {
        portfolio36();
    }
}
