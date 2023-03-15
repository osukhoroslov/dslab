use std::fs;
use std::io::Write;
use std::path::Path;
use std::str::FromStr;

use clap::Parser;
use dslab_dag::dag::DAG;
use env_logger::Builder;

use dslab_dag::dag_simulation::DagSimulation;
use dslab_dag::data_item::DataTransferMode;
use dslab_dag::network::load_network;
use dslab_dag::resource::read_resources;
use dslab_dag::runner::Config;
use dslab_dag::scheduler::{default_scheduler_resolver, SchedulerParams};

const SCHEDULERS: &[&str] = &[
    "Simple",
    "DLS",
    "HEFT",
    "Lookahead",
    "PEFT",
    "Portfolio[algo=0]",
    "Portfolio[algo=1]",
    "Portfolio[algo=2]",
    "Portfolio[algo=3]",
    "Portfolio[algo=4]",
    "Portfolio[algo=5]",
    "Portfolio[algo=6]",
    "Portfolio[algo=7]",
    "Portfolio[algo=8]",
    "Portfolio[algo=9]",
    "Portfolio[algo=10]",
    "Portfolio[algo=21]",
    "Portfolio[algo=22]",
    "Portfolio[algo=23]",
    "Portfolio[algo=24]",
    "Portfolio[algo=25]",
    "Portfolio[algo=26]",
    "Portfolio[algo=27]",
    "Portfolio[algo=28]",
    "Portfolio[algo=29]",
    "Portfolio[algo=30]",
    "Portfolio[algo=31]",
    "Portfolio[algo=32]",
    "Portfolio[algo=33]",
    "Portfolio[algo=34]",
    "Portfolio[algo=35]",
];

#[derive(Parser, Debug)]
#[clap(about, long_about = None)]
/// Runs examples for DSLab DAG
struct Args {
    /// Path to DAG file
    #[clap(short, long)]
    dag: String,

    /// Path to system file
    #[clap(short, long)]
    system: String,

    /// Data transfer mode (direct, via-master-node or manual)
    #[clap(short = 'm', long = "mode", default_value = "direct")]
    data_transfer_mode: String,

    /// Save trace logs in 'traces' dir
    #[clap(short = 't', long = "traces")]
    save_traces: bool,
}

fn main() {
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    let args = Args::parse();
    let dag = DAG::from_file(&args.dag);
    let resources = read_resources(&args.system);
    let network = load_network(&args.system);
    let data_transfer_mode = match args.data_transfer_mode.as_str() {
        "via-master-node" => DataTransferMode::ViaMasterNode,
        "direct" => DataTransferMode::Direct,
        "manual" => DataTransferMode::Manual,
        _ => panic!("Wrong data-transfer-mode"),
    };
    if args.save_traces {
        fs::create_dir_all("traces").expect("Failed to create traces dir");
    }

    println!("\nDAG: {} ({} tasks)", args.dag, dag.get_tasks().len());
    println!("System: {}\n", args.system);
    for scheduler_name in SCHEDULERS.iter() {
        let scheduler_params = SchedulerParams::from_str(scheduler_name).expect("Cannot parse scheduler params");
        let scheduler = default_scheduler_resolver(&scheduler_params).expect("Cannot create scheduler");
        let mut sim = DagSimulation::new(
            123,
            resources.clone(),
            network.clone(),
            scheduler,
            Config { data_transfer_mode },
        );
        let runner = sim.init(dag.clone());
        let trace_path = args.save_traces.then(|| {
            format!(
                "traces/{}_{}_{}_{}.json",
                Path::new(&args.dag).file_stem().unwrap().to_str().unwrap(),
                Path::new(&args.system).file_stem().unwrap().to_str().unwrap(),
                scheduler_name,
                args.data_transfer_mode
            )
        });
        if trace_path.is_some() {
            runner.borrow_mut().enable_trace_log(true);
        }
        sim.step_until_no_events();
        runner.borrow().validate_completed();
        if let Some(trace_path) = trace_path {
            runner.borrow().trace_log().save_to_file(&trace_path).unwrap();
        }
        println!("{:<20}{:.2}", scheduler_name, sim.time());
    }
}
