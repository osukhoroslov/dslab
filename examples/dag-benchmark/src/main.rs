use std::io::Write;
use std::time::Instant;

use clap::Parser;
use env_logger::Builder;
use sugars::{rc, refcell};

use dslab_dag::dag::DAG;
use dslab_dag::dag_simulation::DagSimulation;
use dslab_dag::data_item::DataTransferMode;
use dslab_dag::network::read_network_config;
use dslab_dag::parsers::config::ParserConfig;
use dslab_dag::resource::read_resource_configs;
use dslab_dag::runner::Config;
use dslab_dag::schedulers::simple_scheduler::SimpleScheduler;

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

    /// Save trace log to trace.json
    #[arg(short = 't', long = "trace")]
    save_trace: bool,
}

fn main() {
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    let args = Args::parse();

    let mut sim = DagSimulation::new(
        123,
        read_resource_configs(&args.system),
        read_network_config(&args.system),
        rc!(refcell!(SimpleScheduler::new())),
        Config {
            data_transfer_mode: DataTransferMode::ViaMasterNode,
            billing_interval: 1.,
        },
    );

    let dag = DAG::from_wfcommons(&args.dag, &ParserConfig::with_reference_speed(100.));
    let total_tasks = dag.get_tasks().len();
    let runner = sim.init(dag);
    runner.borrow_mut().enable_trace_log(args.save_trace);

    let t = Instant::now();
    sim.step_until_no_events();
    println!(
        "Processed {} events in {:.2?} ({:.0} events/sec)",
        sim.event_count(),
        t.elapsed(),
        sim.event_count() as f64 / t.elapsed().as_secs_f64()
    );
    println!("Processed {} tasks in {:.3} (simulation time)", total_tasks, sim.time());
    runner.borrow().validate_completed();
    if args.save_trace {
        runner.borrow().trace_log().save_to_file("trace.json").unwrap();
    }

    println!();
}
