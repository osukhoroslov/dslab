mod client;
mod events;
mod worker;

use std::io::Write;
use std::time::Instant;

use clap::Parser;
use env_logger::Builder;
use sugars::{rc, refcell};

use dslab_compute::multicore::Compute;
use simcore::simulation::Simulation;

use crate::client::Client;
use crate::events::Start;
use crate::worker::Worker;

/// Example demonstrating the use of event key getters in async mode
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Number of tasks (>= 1)
    #[clap(long, default_value_t = 10)]
    task_count: u32,
}

fn main() {
    let args = Args::parse();
    let task_count = args.task_count;

    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    // Create simulation and root context
    let mut sim = Simulation::new(123);
    let root = sim.create_context("root");

    // Create worker component which processes tasks
    let compute = rc!(refcell!(Compute::new(
        sim.gen_range(1..=10) as f64,
        sim.gen_range(1..=8) + 8,
        (sim.gen_range(1..=4) + 4) * 1024,
        sim.create_context("compute"),
    )));
    sim.add_handler("compute", compute.clone());
    let worker = rc!(Worker::new(compute, sim.create_context("worker")));
    let worker_id = sim.add_static_handler("worker", worker);

    // Create client component which submits tasks
    let client = rc!(Client::new(sim.create_context("client"), task_count, 100., worker_id));
    sim.add_static_handler("client", client.clone());

    // Start worker and client
    root.emit_now(Start {}, worker_id);
    client.run();

    // Run simulation
    let t = Instant::now();
    sim.step_until_no_events();
    let elapsed = t.elapsed().as_secs_f64();
    println!(
        "Processed {} tasks in {:.2?}s ({:.2} task/s)",
        task_count,
        elapsed,
        task_count as f64 / elapsed
    );
    println!(
        "Processed {} events in {:.2?}s ({:.0} events/s)",
        sim.event_count(),
        elapsed,
        sim.event_count() as f64 / elapsed
    );
}
