mod worker;

use env_logger::Builder;
use std::io::Write;
use worker::{Start, TaskRequest};

use rand::prelude::*;
use rand_pcg::Pcg64;

use sugars::{rc, refcell};

use dslab_compute::multicore::*;
use dslab_core::simulation::Simulation;

use crate::worker::Worker;

fn main() {
    // logger
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    // params
    let task_count = 1;
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

    let worker = rc!(refcell!(Worker::new(compute.clone(), sim.create_context(worker_name))));
    let worker_id = sim.add_handler(worker_name, worker.clone());
    admin.emit_now(Start {}, worker_id);

    Worker::register_key_getters(&sim);

    // submit tasks
    for _ in 0..task_count {
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
