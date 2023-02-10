mod examples;

use std::fs;
use std::io::Write;

use clap::Parser;
use env_logger::Builder;

use dslab_dag::dag_simulation::DagSimulation;
use dslab_dag::data_item::DataTransferMode;
use dslab_dag::network::load_network;
use dslab_dag::runner::Config;
use dslab_dag::scheduler::{default_scheduler_resolver, SchedulerParams};

use crate::examples::{create_example, Example};

const EXAMPLES: &[&str] = &["Diamond", "MapReduce", "Montage", "Epigenomics", "ReuseFiles"];
const SCHEDULERS: &[&str] = &[
    "Simple",
    "HEFT",
    "Lookahead",
    "DLS",
    "PEFT",
    "Portfolio[algo=5]",
    "Portfolio[algo=8]",
];

#[derive(Parser, Debug)]
#[clap(about, long_about = None)]
/// Examples for DSLab DAG
struct Args {
    /// Run specific example
    #[clap(short, long)]
    example: Option<String>,

    /// Data transfer mode (via-master-node, direct or manual)
    #[clap(short = 'm', long = "mode", default_value = "via-master-node")]
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
    let examples = match args.example {
        Some(ref example) => vec![example.as_str()],
        None => EXAMPLES.to_vec(),
    };
    if args.save_traces {
        fs::create_dir_all("traces").expect("Failed to create traces dir");
    }

    println!();
    for example in examples.iter() {
        println!("{:-<40}", example);
        println!("{:<20}Makespan", "");
        for scheduler in SCHEDULERS.iter() {
            let trace_path = args
                .save_traces
                .then(|| format!("traces/{}_{}_{}.json", example, scheduler, args.data_transfer_mode));
            let example = create_example(example);
            let makespan = simulate(example, scheduler, &args.data_transfer_mode, trace_path);
            println!("{:<20}{:.2}", scheduler, makespan);
        }
        println!()
    }
}

fn simulate(example: Example, scheduler: &str, data_transfer_mode: &str, trace_path: Option<String>) -> f64 {
    let scheduler_params = SchedulerParams::from_str(scheduler).expect("Can't parse scheduler params");
    let scheduler = default_scheduler_resolver(&scheduler_params).expect("Cannot create scheduler");

    let data_transfer_mode = match data_transfer_mode {
        "via-master-node" => DataTransferMode::ViaMasterNode,
        "direct" => DataTransferMode::Direct,
        "manual" => DataTransferMode::Manual,
        _ => panic!("Wrong data-transfer-mode"),
    };

    let mut sim = DagSimulation::new(
        123,
        load_network(example.network),
        scheduler,
        Config { data_transfer_mode },
    );
    sim.load_resources(example.resources);

    let runner = sim.init(example.dag);
    if trace_path.is_some() {
        runner.borrow_mut().enable_trace_log(true);
    }

    sim.step_until_no_events();
    runner.borrow().validate_completed();
    if let Some(trace_path) = trace_path {
        runner.borrow().trace_log().save_to_file(&trace_path).unwrap();
    }

    sim.time()
}
