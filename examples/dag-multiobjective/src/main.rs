use std::io::Write;
use std::time::Instant;

use clap::Parser;
use env_logger::Builder;
use sugars::{rc, refcell};

use dslab_dag::dag::DAG;
use dslab_dag::dag_simulation::DagSimulation;
use dslab_dag::data_item::DataTransferMode;
use dslab_dag::network::read_network_config;
use dslab_dag::pareto::ParetoSimulation;
use dslab_dag::pareto_schedulers::moheft::MOHeftScheduler;
use dslab_dag::parsers::config::ParserConfig;
use dslab_dag::resource::read_resource_configs;
use dslab_dag::runner::Config;
use dslab_dag::schedulers::heft::HeftScheduler;

#[derive(Parser, Debug)]
#[command(about, long_about = None)]
/// Runs DSLab DAG benchmark
struct Args {
    /// Path to DAG file in WfCommons-3 format
    #[arg(short, long)]
    dag: String,

    /// Path to system file
    #[arg(short, long)]
    system: String,
}

fn main() {
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    let args = Args::parse();

    let mut heft_sim = DagSimulation::new(
        123,
        read_resource_configs(&args.system),
        read_network_config(&args.system),
        rc!(refcell!(HeftScheduler::new())),
        Config {
            data_transfer_mode: DataTransferMode::Direct,
            pricing_interval: 3600.,
        },
    );

    let dag = DAG::from_wfcommons(&args.dag, &ParserConfig::with_reference_speed(100.));
    let total_tasks = dag.get_tasks().len();
    let runner = heft_sim.init(dag.clone());

    let t = Instant::now();
    heft_sim.step_until_no_events();
    runner.borrow().validate_completed();
    println!(
        "HEFT makespan = {:.3} cost = {:.3}",
        runner.borrow().run_stats().makespan,
        runner.borrow().run_stats().total_execution_cost
    );

    let moheft_sim = ParetoSimulation::new(
        dag,
        read_resource_configs(&args.system),
        read_network_config(&args.system),
        rc!(refcell!(MOHeftScheduler::new(16))),
        DataTransferMode::Direct,
        Some(3600.0),
    );
    let results = moheft_sim.run(8);
    println!("MOHEFT solutions:");
    for result in &results.run_stats {
        println!(
            "makespan = {:.3} cost = {:.3}",
            result.makespan, result.total_execution_cost
        );
    }
}
