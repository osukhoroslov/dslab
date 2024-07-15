use std::io::Write;

use clap::Parser;
use env_logger::Builder;
use rand::prelude::*;
use rand_pcg::Pcg64;
use sugars::{rc, refcell};

use dslab_compute::multicore::*;
use simcore::simulation::Simulation;

mod worker;
use crate::worker::{TaskRequest, Worker};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Number of tasks (>= 1)
    #[clap(long, default_value_t = 10)]
    task_count: u32,
}

fn main() {
    let args = Args::parse();
    // logger
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    // params
    let seed = 123;

    let mut sim = Simulation::new(seed);
    let mut rand = Pcg64::seed_from_u64(seed);

    // admin context for starting master and workers
    let admin = sim.create_context("admin");

    // create and start worker
    let host = "host_0";
    let worker_name = &format!("{}::worker", host);

    let compute_name = format!("{}::compute", host);
    let compute = rc!(refcell!(Compute::new(
        rand.gen_range(1..=10) as f64,
        rand.gen_range(1..=8),
        rand.gen_range(1..=4) * 1024,
        sim.create_context(&compute_name),
    )));
    sim.add_handler(compute_name, compute.clone());

    let worker = Worker::new(compute.clone(), sim.create_context(worker_name));
    let worker_id = sim.add_static_handler(worker_name, rc!(worker));

    Worker::register_key_getters(&sim);

    // submit tasks
    for _ in 0..args.task_count {
        let task = TaskRequest {
            flops: rand.gen_range(100..=1000) as f64,
            memory: rand.gen_range(1..=8) * 128,
            min_cores: 1,
            max_cores: 1,
            cores_dependency: CoresDependency::Linear,
        };
        admin.emit_now(task, worker_id);
    }

    sim.step_until_no_events();
}
