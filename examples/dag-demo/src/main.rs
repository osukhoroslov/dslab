use std::fs;
use std::io::Write;
use std::path::Path;
use std::str::FromStr;

use clap::Parser;
use dslab_dag::dag::DAG;
use env_logger::Builder;

use dslab_dag::dag_simulation::DagSimulation;
use dslab_dag::data_item::DataTransferMode;
use dslab_dag::network::read_network_config;
use dslab_dag::parsers::config::ParserConfig;
use dslab_dag::resource::read_resource_configs;
use dslab_dag::runner::Config;
use dslab_dag::scheduler::{default_scheduler_resolver, SchedulerParams};

const ALGORITHMS: &[&str] = &[
    "Simple",
    "DLS",
    "HEFT",
    "Lookahead[depth=0]",                  // Identical to HEFT
    "Lookahead[depth=1,depth_mode=Local]", // Original Lookahead version considering only task's children
    "Lookahead[depth=1]",                  // Lookahead version considering all ready unscheduled tasks (depth=1)
    "Lookahead[depth=2]",                  // Lookahead version considering all unscheduled tasks with depth 2
    "Lookahead[depth=3]",                  // Lookahead version considering all unscheduled tasks with depth 3
    "Lookahead",                           // Lookahead version considering all unscheduled tasks
    "PEFT",
    "DynamicList[task=CompSize,resource=Speed,cores=MaxCores]",
    "DynamicList[task=CompSize,resource=Speed,cores=Efficiency90]",
    "DynamicList[task=CompSize,resource=Speed,cores=Efficiency50]",
    "DynamicList[task=CompSize,resource=TaskData,cores=MaxCores]",
    "DynamicList[task=CompSize,resource=TaskData,cores=Efficiency90]",
    "DynamicList[task=CompSize,resource=TaskData,cores=Efficiency50]",
    "DynamicList[task=CompSize,resource=MaxAvailableCores,cores=MaxCores]",
    "DynamicList[task=CompSize,resource=MaxAvailableCores,cores=Efficiency90]",
    "DynamicList[task=CompSize,resource=MaxAvailableCores,cores=Efficiency50]",
    "DynamicList[task=DataSize,resource=Speed,cores=MaxCores]",
    "DynamicList[task=DataSize,resource=Speed,cores=Efficiency90]",
    "DynamicList[task=DataSize,resource=Speed,cores=Efficiency50]",
    "DynamicList[task=DataSize,resource=TaskData,cores=MaxCores]",
    "DynamicList[task=DataSize,resource=TaskData,cores=Efficiency90]",
    "DynamicList[task=DataSize,resource=TaskData,cores=Efficiency50]",
    "DynamicList[task=DataSize,resource=MaxAvailableCores,cores=MaxCores]",
    "DynamicList[task=DataSize,resource=MaxAvailableCores,cores=Efficiency90]",
    "DynamicList[task=DataSize,resource=MaxAvailableCores,cores=Efficiency50]",
    "DynamicList[task=ChildrenCount,resource=Speed,cores=MaxCores]",
    "DynamicList[task=ChildrenCount,resource=Speed,cores=Efficiency90]",
    "DynamicList[task=ChildrenCount,resource=Speed,cores=Efficiency50]",
    "DynamicList[task=ChildrenCount,resource=TaskData,cores=MaxCores]",
    "DynamicList[task=ChildrenCount,resource=TaskData,cores=Efficiency90]",
    "DynamicList[task=ChildrenCount,resource=TaskData,cores=Efficiency50]",
    "DynamicList[task=ChildrenCount,resource=MaxAvailableCores,cores=MaxCores]",
    "DynamicList[task=ChildrenCount,resource=MaxAvailableCores,cores=Efficiency90]",
    "DynamicList[task=ChildrenCount,resource=MaxAvailableCores,cores=Efficiency50]",
    "DynamicList[task=BottomLevel,resource=Speed,cores=MaxCores]",
    "DynamicList[task=BottomLevel,resource=Speed,cores=Efficiency90]",
    "DynamicList[task=BottomLevel,resource=Speed,cores=Efficiency50]",
    "DynamicList[task=BottomLevel,resource=TaskData,cores=MaxCores]",
    "DynamicList[task=BottomLevel,resource=TaskData,cores=Efficiency90]",
    "DynamicList[task=BottomLevel,resource=TaskData,cores=Efficiency50]",
    "DynamicList[task=BottomLevel,resource=MaxAvailableCores,cores=MaxCores]",
    "DynamicList[task=BottomLevel,resource=MaxAvailableCores,cores=Efficiency90]",
    "DynamicList[task=BottomLevel,resource=MaxAvailableCores,cores=Efficiency50]",
];

#[derive(Parser, Debug)]
#[command(about, long_about = None)]
/// Simulates the DAG execution using different scheduling algorithms and outputs the obtained makespans.
struct Args {
    /// Path to DAG file
    #[arg(short, long)]
    dag: String,

    /// Path to system file
    #[arg(short, long)]
    system: String,

    /// Data transfer mode (direct, via-master-node or manual)
    #[arg(short = 'm', long = "mode", default_value = "direct")]
    data_transfer_mode: String,

    /// Save trace logs in 'traces' dir
    #[arg(short = 't', long = "traces")]
    save_traces: bool,
}

fn main() {
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    let args = Args::parse();
    let dag = DAG::from_file(&args.dag, &ParserConfig::default());
    let resource_configs = read_resource_configs(&args.system);
    let network_config = read_network_config(&args.system);
    let data_transfer_mode = match args.data_transfer_mode.as_str() {
        "via-master-node" => DataTransferMode::ViaMasterNode,
        "direct" => DataTransferMode::Direct,
        "manual" => DataTransferMode::Manual,
        _ => panic!("Wrong data-transfer-mode"),
    };
    if args.save_traces {
        fs::create_dir_all("traces").expect("Failed to create traces dir");
    }

    let algorithm_width = ALGORITHMS.iter().map(|alg| alg.len()).max().unwrap();

    println!("\nDAG: {} ({} tasks)", args.dag, dag.get_tasks().len());
    println!("System: {}\n", args.system);
    for algorithm in ALGORITHMS.iter() {
        let scheduler_params = SchedulerParams::from_str(algorithm).expect("Cannot parse scheduler params");
        let scheduler = default_scheduler_resolver(&scheduler_params).expect("Cannot create scheduler");
        let mut sim = DagSimulation::new(
            123,
            resource_configs.clone(),
            network_config.clone(),
            scheduler,
            Config {
                data_transfer_mode,
                billing_interval: 1.,
            },
        );
        let runner = sim.init(dag.clone());
        let trace_path = args.save_traces.then(|| {
            format!(
                "traces/{}_{}_{}_{}.json",
                Path::new(&args.dag).file_stem().unwrap().to_str().unwrap(),
                Path::new(&args.system).file_stem().unwrap().to_str().unwrap(),
                algorithm,
                args.data_transfer_mode
            )
        });
        if trace_path.is_some() {
            runner.borrow_mut().enable_trace_log(true);
        }
        let now = std::time::Instant::now();
        sim.step_until_no_events();
        runner.borrow().validate_completed();
        if let Some(trace_path) = trace_path {
            runner.borrow().trace_log().save_to_file(&trace_path).unwrap();
        }
        println!(
            "{:>algorithm_width$}  {: >8.2}  {: >8.2?}",
            algorithm,
            sim.time(),
            now.elapsed()
        );
    }
}
