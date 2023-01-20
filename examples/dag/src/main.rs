use std::fs;
use std::io::Write;

use clap::Parser;
use env_logger::Builder;
use indexmap::IndexMap;
use rand::prelude::*;
use rand_pcg::Pcg64;

use dslab_compute::multicore::*;
use dslab_dag::dag::DAG;
use dslab_dag::dag_simulation::DagSimulation;
use dslab_dag::data_item::DataTransferMode;
use dslab_dag::network::load_network;
use dslab_dag::resource::read_resources;
use dslab_dag::runner::Config;
use dslab_dag::scheduler::{default_scheduler_resolver, SchedulerParams};

struct RunParams {
    scheduler: String,
    data_transfer_mode: DataTransferMode,
    trace_path: Option<String>,
}

fn run_simulation(dag: DAG, resources_file: &str, network_file: &str, params: RunParams) -> f64 {
    let scheduler_params = SchedulerParams::from_str(&params.scheduler).expect("Can't parse scheduler params");
    let scheduler = default_scheduler_resolver(&scheduler_params).expect("Cannot create scheduler");

    let mut sim = DagSimulation::new(
        123,
        read_resources(resources_file),
        load_network(network_file),
        scheduler,
        Config {
            data_transfer_mode: params.data_transfer_mode,
        },
    );

    let runner = sim.init(dag);
    if params.trace_path.is_some() {
        runner.borrow_mut().enable_trace_log(true);
    }

    sim.step_until_no_events();
    runner.borrow().validate_completed();
    if let Some(trace_path) = params.trace_path {
        runner.borrow().trace_log().save_to_file(&trace_path).unwrap();
    }

    sim.time()
}

fn map_reduce(params: RunParams) -> f64 {
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

    run_simulation(dag, "resources/cluster1.yaml", "networks/network1.yaml", params)
}

fn epigenomics(params: RunParams) -> f64 {
    run_simulation(
        DAG::from_dax("dags/Epigenomics_100.xml", 1000.),
        "resources/cluster2.yaml",
        "networks/network2.yaml",
        params,
    )
}

fn montage(params: RunParams) -> f64 {
    run_simulation(
        DAG::from_dot("dags/Montage.dot"),
        "resources/cluster2.yaml",
        "networks/network3.yaml",
        params,
    )
}

fn diamond(params: RunParams) -> f64 {
    run_simulation(
        DAG::from_yaml("dags/diamond.yaml"),
        "resources/cluster3.yaml",
        "networks/network4.yaml",
        params,
    )
}

fn reuse_files(params: RunParams) -> f64 {
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

    run_simulation(dag, "resources/cluster1.yaml", "networks/network1.yaml", params)
}

#[derive(Parser, Debug)]
#[clap(about, long_about = None)]
struct Args {
    /// Run a single experiment (e.g. "Montage")
    #[clap(short, long)]
    experiment: Option<String>,

    /// Run a single scheduler (Simple, HEFT, Lookahead, etc.)
    #[clap(short, long)]
    scheduler: Option<String>,

    /// Data transfer mode (via-master-node, direct or manual)
    #[clap(short = 'm', long = "mode", default_value = "via-master-node")]
    data_transfer_mode: String,

    /// Save trace logs to files in 'traces' dir
    #[clap(short = 't', long = "traces")]
    save_traces: bool,
}

fn main() {
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    let args = Args::parse();
    let data_transfer_mode = match args.data_transfer_mode.as_str() {
        "via-master-node" => DataTransferMode::ViaMasterNode,
        "direct" => DataTransferMode::Direct,
        "manual" => DataTransferMode::Manual,
        _ => panic!("Wrong data-transfer-mode"),
    };
    if args.save_traces {
        fs::create_dir_all("traces").expect("Failed to create traces dir");
    }

    let mut experiment_fns: IndexMap<&str, fn(RunParams) -> f64> = IndexMap::new();
    experiment_fns.insert("Diamond", diamond);
    experiment_fns.insert("MapReduce", map_reduce);
    experiment_fns.insert("Montage", montage);
    experiment_fns.insert("Epigenomics", epigenomics);
    experiment_fns.insert("ReuseFiles", reuse_files);

    let experiments = match args.experiment {
        Some(ref experiment) => vec![experiment.as_str()],
        None => experiment_fns.keys().cloned().collect(),
    };

    let schedulers = match args.scheduler {
        Some(ref scheduler) => vec![scheduler.as_str()],
        None => vec!["Simple", "HEFT", "Lookahead", "DLS", "PEFT"],
    };

    for experiment in experiments.iter() {
        let experiment_fn = experiment_fns.get(experiment).unwrap();
        println!("{} ------------------------------------------\n", experiment);
        for scheduler in schedulers.iter() {
            let params = RunParams {
                scheduler: scheduler.to_string(),
                data_transfer_mode,
                trace_path: args
                    .save_traces
                    .then(|| format!("traces/{}_{}_{}.json", experiment, scheduler, args.data_transfer_mode)),
            };
            let makespan = experiment_fn(params);
            println!("Scheduler: {}", scheduler);
            println!("Makespan: {:.2}\n", makespan);
        }
    }
}
