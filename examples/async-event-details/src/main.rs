mod events;
mod process;

use std::io::Write;

use std::time::Instant;

use clap::Parser;
use env_logger::Builder;
use events::{get_compute_failed_id, get_compute_finished_id, get_compute_start_id, Start, TaskRequest};
use process::Worker;
use rand::prelude::*;
use rand_pcg::Pcg64;
use sugars::{rc, refcell};

use dslab_compute::multicore::{CompFailed, CompFinished, CompStarted, Compute};
use dslab_core::{simulation::Simulation, Id, SimulationContext};

use crate::process::TaskInfo;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Number of tasks (>= 1)
    #[clap(long, default_value_t = 10)]
    tasks_count: u32,
}

struct Client {
    ctx: SimulationContext,
    max_task_delay: f64,
    task_count: u32,
    worker_id: Id,
}

impl Client {
    fn new(ctx: SimulationContext, max_task_delay: f64, task_count: u32, worker_id: Id) -> Self {
        Self {
            ctx,
            max_task_delay,
            task_count,
            worker_id,
        }
    }

    fn run(&self) {
        self.ctx.spawn(self.submit_tasks())
    }

    async fn submit_tasks(&self) {
        for _i in 0..self.task_count {
            let flops = self.ctx.gen_range(1..=3000) as f64;
            let cores = self.ctx.gen_range(1..=8) as u32;
            let memory = self.ctx.gen_range(1..=4) * 1024_u64;

            self.ctx.emit_now(TaskRequest { flops, cores, memory }, self.worker_id);

            self.ctx
                .async_wait_for(self.ctx.gen_range(1.0..=self.max_task_delay))
                .await;
        }
    }
}

fn register_deails_getters(sim: &Simulation) {
    sim.register_details_getter_for::<CompStarted>(get_compute_start_id);
    sim.register_details_getter_for::<CompFailed>(get_compute_failed_id);
    sim.register_details_getter_for::<CompFinished>(get_compute_finished_id);
}

fn main() {
    let args = Args::parse();
    let task_count = args.tasks_count;

    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    let seed = 42;
    let mut sim = Simulation::new(seed);
    let mut rand = Pcg64::seed_from_u64(seed);
    // admin context for starting master and workers
    let admin = sim.create_context("admin");

    let host = "host";

    let compute_name = format!("{}::compute", host);
    let worker_name = format!("{}:worker", host);
    let worker_chan_name = format!("{}:task_channel", &worker_name);

    let compute_context = sim.create_context(&compute_name);

    let compute_id = compute_context.id();

    let compute = rc!(refcell!(Compute::new(
        rand.gen_range(1..=10) as f64,
        rand.gen_range(1..=8) + 8,
        (rand.gen_range(1..=4) + 4) * 1024,
        compute_context,
    )));

    sim.add_handler(compute_name, compute.clone());

    let worker = rc!(refcell!(Worker::new(
        compute,
        compute_id,
        sim.create_context(&worker_name),
        sim.create_channel::<TaskInfo, &String>(&worker_chan_name),
    )));

    sim.add_handler(worker_name, worker.clone());

    register_deails_getters(&sim);

    admin.emit_now(Start {}, worker.borrow().id());

    // client context for submitting tasks
    let client = Client::new(sim.create_context("client"), 100., task_count, worker.borrow().id());
    client.run();

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
