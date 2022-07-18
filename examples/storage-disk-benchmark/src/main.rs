mod random;

use std::cell::RefCell;
use std::io::Write;
use std::rc::Rc;
use std::time::Instant;

use clap::Parser;
use env_logger::Builder;
use random::CustomRandom;
use serde::Serialize;
use sugars::{rc, refcell};

use dslab_core::context::SimulationContext;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;
use dslab_core::simulation::Simulation;
use dslab_core::{cast, log_debug, log_error, log_info};

use dslab_storage::events::{DataReadCompleted, DataReadFailed};
use dslab_storage::shared_disk::SharedDisk;

const SEED: u64 = 16;
const DISK_CAPACITY: u64 = 10u64.pow(10);
const DISK_READ_BW: f64 = 100.;
const DISK_WRITE_BW: f64 = 100.;

/// Disk benchmark
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Number of requests (>= 1)
    #[clap(long, default_value_t = 1)]
    requests: u64,

    /// Number of disks (>= 1)
    #[clap(long, default_value_t = 1)]
    disks: u64,

    /// Maximal size (>= 1)
    #[clap(long, default_value_t = 10u64.pow(9) + 6)]
    max_size: u64,

    /// Maximal activity start time (0 by default, so all will start at 0)
    #[clap(long, default_value_t = 0)]
    max_start_time: u64,
}

struct Runner {
    disks: Vec<Rc<RefCell<SharedDisk>>>,
    ctx: SimulationContext,
    requests_count: u64,
    max_size: u64,
}

#[derive(Serialize)]
struct TimerFired {
    disk_idx: usize,
    size: u64,
}

struct DiskRequest {
    pub disk_idx: usize,
    pub start_time: u64,
    pub size: u64,
}

impl DiskRequest {
    fn new(disk_idx: usize, start_time: u64, size: u64) -> Self {
        Self {
            disk_idx,
            start_time,
            size,
        }
    }
}

fn generate_requests(disks_count: u64, requests_count: u64, max_size: u64, max_start_time: u64) -> Vec<DiskRequest> {
    let mut rnd = CustomRandom::new(SEED);
    let mut requests = vec![];
    for _ in 0..requests_count {
        let disk_idx = rnd.next() % disks_count;
        let start_time = rnd.next() % (max_start_time + 1);
        let size = rnd.next() % (max_size + 1);
        requests.push(DiskRequest::new(disk_idx as usize, start_time, size));
    }
    requests
}

impl Runner {
    fn new(disks: Vec<Rc<RefCell<SharedDisk>>>, ctx: SimulationContext) -> Self {
        Self {
            disks,
            ctx,
            requests_count: 0,
            max_size: 0,
        }
    }

    fn start(&mut self, requests_count: u64, max_size: u64, max_start_time: u64) {
        log_info!(self.ctx, "Starting disk benchmark");
        let requests = generate_requests(self.disks.len() as u64, requests_count, max_size, max_start_time);
        self.requests_count = requests_count;
        self.max_size = max_size;

        for idx in 0..self.requests_count {
            let request = requests.get(idx as usize).unwrap();
            self.ctx.emit_self(
                TimerFired {
                    disk_idx: request.disk_idx,
                    size: request.size,
                },
                request.start_time as f64,
            );
        }
    }

    fn on_timer_fired(&mut self, disk_idx: usize, size: u64) {
        self.disks.get(disk_idx).unwrap().borrow_mut().read(size, self.ctx.id());
    }
}

impl EventHandler for Runner {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            DataReadCompleted { request_id: _, size } => {
                self.requests_count -= 1;
                log_debug!(
                    self.ctx,
                    "Completed reading from {}, size = {}",
                    self.ctx.lookup_name(event.src),
                    size,
                );
            }
            DataReadFailed { request_id: _, error } => {
                log_error!(self.ctx, "Unexpected error: {}", error);
            }
            TimerFired { disk_idx, size } => {
                self.on_timer_fired(disk_idx, size);
            }
        })
    }
}

fn main() {
    let args = Args::parse();

    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    let mut sim = Simulation::new(SEED);
    let mut disks = vec![];

    for i in 0..args.disks {
        let disk_name = format!("disk-{}", i);
        let disk = rc!(refcell!(SharedDisk::new_simple(
            DISK_CAPACITY,
            DISK_READ_BW,
            DISK_WRITE_BW,
            sim.create_context(disk_name.clone()),
        )));
        sim.add_handler(disk_name, disk.clone());
        disks.push(disk);
    }

    let runner = rc!(refcell!(Runner::new(disks, sim.create_context("runner"))));
    sim.add_handler("runner", runner.clone());
    runner
        .borrow_mut()
        .start(args.requests, args.max_size, args.max_start_time);

    let t = Instant::now();
    sim.step_until_no_events();
    let elapsed = t.elapsed().as_millis();
    println!(
        "Processed {} requests in {:.2?} ms ({:.0} act/s)",
        args.requests,
        elapsed,
        args.requests as f64 / elapsed as f64 * 1000.
    );
    println!(
        "Processed {} events in {:.2?} ms ({:.0} events/s)",
        sim.event_count(),
        elapsed,
        sim.event_count() as f64 / elapsed as f64 * 1000.
    );
    println!("Finish");
}
