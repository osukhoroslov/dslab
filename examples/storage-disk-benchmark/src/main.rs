mod random;

use std::cell::RefCell;
use std::collections::HashMap;
use std::io::Write;
use std::rc::Rc;
use std::time::Instant;

use clap::Parser;
use env_logger::Builder;
use random::CustomRandom;
use serde::Serialize;
use sugars::{rc, refcell};

use dslab_core::component::Id;
use dslab_core::context::SimulationContext;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;
use dslab_core::simulation::Simulation;
use dslab_core::{cast, log_error, log_info};

use dslab_storage::disk::Disk;
use dslab_storage::events::{DataReadCompleted, DataReadFailed};
use dslab_storage::storage::Storage;

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

    /// Maximal request start time (0 by default, so all will start at 0)
    #[clap(long, default_value_t = 0)]
    max_start_time: u64,
}

struct Runner {
    disks: Vec<(Id, Rc<RefCell<Disk>>)>,
    ctx: SimulationContext,
    requests_count: u64,
    max_size: u64,
    requests: Vec<DiskRequest>,
    request_start_times: HashMap<(Id, u64), (f64, usize)>, /* (disk_id, disk_request_id) ->
                                                            * (disk_request_start_time, runner_request_id) */
}

#[derive(Clone, Serialize)]
struct TimerFired {
    request_idx: usize,
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
    // Need to sort for equality to SimGrid, where this order is needed
    requests
        .sort_by(|lhs, rhs| (lhs.start_time, lhs.disk_idx, lhs.size).cmp(&(rhs.start_time, rhs.disk_idx, rhs.size)));
    requests
}

impl Runner {
    fn new(disks: Vec<(Id, Rc<RefCell<Disk>>)>, ctx: SimulationContext) -> Self {
        Self {
            disks,
            ctx,
            requests_count: 0,
            max_size: 0,
            requests: Vec::new(),
            request_start_times: HashMap::new(),
        }
    }

    fn start(&mut self, requests_count: u64, max_size: u64, max_start_time: u64) {
        log_info!(self.ctx, "Starting disk benchmark");
        self.requests = generate_requests(self.disks.len() as u64, requests_count, max_size, max_start_time);
        self.requests_count = requests_count;
        self.max_size = max_size;

        for request_idx in 0..self.requests_count as usize {
            let request = self.requests.get(request_idx).unwrap();
            self.ctx
                .emit_self(TimerFired { request_idx }, request.start_time as f64);
        }
    }

    fn on_timer_fired(&mut self, request_idx: usize) {
        let req = self.requests.get(request_idx).unwrap();
        let (disk_id, disk) = self.disks.get(req.disk_idx).unwrap();
        let disk_request_id = disk.borrow_mut().read(req.size, self.ctx.id());
        self.request_start_times
            .insert((*disk_id, disk_request_id), (self.ctx.time(), request_idx));
        log_info!(
            self.ctx,
            "Starting request #{}: read from disk-{}, size = {}, expected start time = {:.3}",
            request_idx,
            req.disk_idx,
            req.size,
            req.start_time as f64
        )
    }
}

impl EventHandler for Runner {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            DataReadCompleted { request_id, size } => {
                self.requests_count -= 1;
                let (start_time, request_idx) = self.request_start_times.get(&(event.src, request_id)).unwrap();
                log_info!(
                    self.ctx,
                    "Completed request #{}: read from {}, size = {}, elapsed simulation time = {:.3}",
                    request_idx,
                    self.ctx.lookup_name(event.src),
                    size,
                    self.ctx.time() - start_time,
                );
            }
            DataReadFailed { request_id: _, error } => {
                log_error!(self.ctx, "Unexpected error: {}", error);
            }
            TimerFired { request_idx } => {
                self.on_timer_fired(request_idx);
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

        let disk = rc!(refcell!(Disk::simple(
            DISK_CAPACITY,
            DISK_READ_BW,
            DISK_WRITE_BW,
            sim.create_context(&disk_name),
        )));

        disks.push((sim.add_handler(disk_name, disk.clone()), disk));
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
        "Processed {} requests in {:.2?} ms ({:.0} requests/s)",
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
