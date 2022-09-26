use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io::Write;
use std::rc::Rc;
use std::time::Instant;

use clap::Parser;

use sugars::{rc, refcell};

use rand::prelude::*;
use rand_pcg::Pcg64;

use env_logger::Builder;

use dslab_compute::multicore::*;
use dslab_dag::dag::DAG;
use dslab_dag::dag_simulation::DagSimulation;
use dslab_dag::data_item::{DataTransferMode, DataTransferStrategy};
use dslab_dag::network::load_network;
use dslab_dag::runner::Config;
use dslab_dag::scheduler::Scheduler;
use dslab_dag::schedulers::heft::HeftScheduler;
use dslab_dag::schedulers::lookahead::LookaheadScheduler;
use dslab_dag::schedulers::simple_scheduler::SimpleScheduler;
use dslab_dag::schedulers::simple_with_data::SimpleDataScheduler;

#[derive(Parser, Debug)]
#[clap(about, long_about = None)]
struct Args {
    /// Run only one experiment
    #[clap(long = "run-one")]
    run_one: Option<String>,

    /// Save trace_log to a file
    #[clap(long = "trace-log")]
    trace_log: bool,

    /// Scheduler [heft, simple, simple-with-data]
    #[clap(long, default_value = "heft")]
    scheduler: String,

    /// Data transfer mode (via-master-node, direct or manual)
    #[clap(long = "data-transfer-mode", default_value = "via-master-node")]
    data_transfer_mode: String,
}

fn run_simulation(args: &Args, dag: DAG, resources_file: &str, network_file: &str, trace_file: &str) {
    let network_model = load_network(network_file);

    let enable_trace_log = args.trace_log;
    let scheduler: Rc<RefCell<dyn Scheduler>> = match args.scheduler.as_str() {
        "simple" => rc!(refcell!(SimpleScheduler::new())),
        "simple-with-data" => rc!(refcell!(SimpleDataScheduler::new())),
        "heft" => {
            rc!(refcell!(
                HeftScheduler::new().with_data_transfer_strategy(DataTransferStrategy::Eager)
            ))
        }
        "lookahead" => {
            rc!(refcell!(
                LookaheadScheduler::new().with_data_transfer_strategy(DataTransferStrategy::Eager)
            ))
        }
        _ => {
            eprintln!("Wrong scheduler");
            std::process::exit(1);
        }
    };

    let data_transfer_mode = match args.data_transfer_mode.as_str() {
        "via-master-node" => DataTransferMode::ViaMasterNode,
        "direct" => DataTransferMode::Direct,
        "manual" => DataTransferMode::Manual,
        _ => {
            eprintln!("Wrong data-transfer-mode");
            std::process::exit(1);
        }
    };
    let mut sim = DagSimulation::new(123, network_model, scheduler, Config { data_transfer_mode });
    sim.load_resources(resources_file);

    let runner = sim.init(dag);
    runner.borrow_mut().enable_trace_log(enable_trace_log);

    let t = Instant::now();
    sim.step_until_no_events();
    println!("Makespan: {:.2}", sim.time());
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

fn map_reduce(args: &Args) {
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
        args,
        dag,
        "resources/cluster1.yaml",
        "networks/network1.yaml",
        "traces/trace_map_reduce.json",
    );
}

fn epigenomics(args: &Args) {
    run_simulation(
        args,
        DAG::from_dax("dags/Epigenomics_100.xml", 1000.),
        "resources/cluster2.yaml",
        "networks/network2.yaml",
        "traces/trace_epigenomics.json",
    );
}

fn montage(args: &Args) {
    run_simulation(
        args,
        DAG::from_dot("dags/Montage.dot"),
        "resources/cluster2.yaml",
        "networks/network3.yaml",
        "traces/trace_montage.json",
    );
}

fn diamond(args: &Args) {
    run_simulation(
        args,
        DAG::from_yaml("dags/diamond.yaml"),
        "resources/cluster3.yaml",
        "networks/network4.yaml",
        "traces/trace_diamond.json",
    );
}

fn reuse_files(args: &Args) {
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
        args,
        dag,
        "resources/cluster1.yaml",
        "networks/network1.yaml",
        "traces/trace_reuse_files.json",
    );
}

fn main() {
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    let args = Args::parse();

    let mut experiments: BTreeMap<String, fn(&Args)> = BTreeMap::new();
    experiments.insert("map_reduce".to_string(), map_reduce);
    experiments.insert("epigenomics".to_string(), epigenomics); // dax
    experiments.insert("montage".to_string(), montage); // dot
    experiments.insert("diamond".to_string(), diamond); // yaml
    experiments.insert("reuse_files".to_string(), reuse_files);

    if args.run_one.is_some() {
        let name = args.run_one.as_ref().unwrap();
        println!("Running {}", name);
        experiments.get(name).unwrap()(&args);
    } else {
        for (name, experiment) in experiments.into_iter() {
            println!("Running {}", name);
            experiment(&args);
        }
    }
}
