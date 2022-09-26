use std::io::Write;
use std::time::Instant;

use clap::{arg, command, Arg};
use env_logger::Builder;
use sugars::{rc, refcell};

use dslab_dag::dag::DAG;
use dslab_dag::dag_simulation::DagSimulation;
use dslab_dag::data_item::DataTransferMode;
use dslab_dag::network::load_network;
use dslab_dag::runner::Config;
use dslab_dag::schedulers::simple_scheduler::SimpleScheduler;

fn main() {
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    let matches = command!()
        .arg(arg!([SYSTEM]).help("Yaml file with resources and network"))
        .arg(arg!([WORKFLOW]).help("File with workflow in WfCommons-3 format"))
        .arg(
            Arg::new("trace-log")
                .long("trace-log")
                .help("Save trace_log to file")
                .takes_value(false),
        )
        .get_matches();

    let system_file = matches.value_of("SYSTEM").unwrap();
    let dag_file = matches.value_of("WORKFLOW").unwrap();

    let enable_trace_log = matches.is_present("trace-log");

    let mut sim = DagSimulation::new(
        123,
        load_network(system_file),
        rc!(refcell!(SimpleScheduler::new())),
        Config {
            data_transfer_mode: DataTransferMode::ViaMasterNode,
        },
    );
    sim.load_resources(system_file);

    let dag = DAG::from_wfcommons(dag_file, 1.0e11);
    let total_tasks = dag.get_tasks().len();
    let runner = sim.init(dag);
    runner.borrow_mut().enable_trace_log(enable_trace_log);

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
    if enable_trace_log {
        runner.borrow().trace_log().save_to_file("trace.json").unwrap();
    }

    println!();
}
