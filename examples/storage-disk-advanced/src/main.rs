use std::cell::RefCell;
use std::collections::HashMap;
use std::io::Write;
use std::rc::Rc;

use env_logger::Builder;
use rand::distributions::Uniform;
use serde::Serialize;
use sugars::{boxed, rc, refcell};

use dslab_core::cast;
use dslab_core::context::SimulationContext;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;
use dslab_core::log_info;
use dslab_core::simulation::Simulation;

use dslab_models::throughput_sharing::{make_constant_throughput_fn, make_uniform_factor_fn, ActivityFactorFn};
use dslab_storage::disk::{Disk, DiskActivity, DiskSpec};
use dslab_storage::events::{DataReadCompleted, DataReadFailed, DataWriteCompleted, DataWriteFailed};
use dslab_storage::storage::Storage;

const SEED: u64 = 16;

const SIMPLE_DISK_NAME: &str = "SimpleDisk";
const ADVANCED_DISK_NAME: &str = "AdvancedDisk";

const SIMPLE_CLIENT_NAME: &str = "SimpleClient";
const ADVANCED_CLIENT_NAME: &str = "AdvancedClient";

const DISK_CAPACITY: u64 = 1000;
const DISK_READ_BW: f64 = 100.;
const DISK_WRITE_BW: f64 = 100.;

const READ_ITERATIONS: u64 = 100;
const WRITE_ITERATIONS: u64 = 100;

struct Client {
    disk: Rc<RefCell<Disk>>,
    requests: HashMap<u64, u64>, // request_id -> test case
    ctx: SimulationContext,
    start_time: f64,
}

#[derive(Clone, Serialize)]
struct Start {}

#[derive(Clone, Serialize)]
struct Ticker {}

impl Client {
    fn new(disk: Rc<RefCell<Disk>>, ctx: SimulationContext) -> Self {
        Self {
            disk,
            requests: HashMap::new(),
            ctx,
            start_time: 0.,
        }
    }
}

struct ExampleDiskFactorFn {}

impl ActivityFactorFn<DiskActivity> for ExampleDiskFactorFn {
    fn get_factor(&mut self, item: &DiskActivity, ctx: &mut SimulationContext) -> f64 {
        if item.size < 10 {
            return 1.;
        }
        ctx.sample_from_distribution(&Uniform::<f64>::new(0.9, 1.))
    }
}

impl EventHandler for Client {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            Start {} => {
                self.start_time = self.ctx.time();
                for i in 0..READ_ITERATIONS {
                    self.requests.insert(self.disk.borrow_mut().read(10, self.ctx.id()), i);
                }
                for i in 0..WRITE_ITERATIONS {
                    self.requests
                        .insert(self.disk.borrow_mut().write(10, self.ctx.id()), READ_ITERATIONS + i);
                }
            }
            DataReadCompleted { request_id, size: _ } => {
                log_info!(
                    self.ctx,
                    "Read iteration #{} completed. Elapsed time = {}",
                    self.requests[&request_id],
                    self.ctx.time() - self.start_time,
                );
            }
            DataReadFailed { request_id, error } => {
                log_info!(
                    self.ctx,
                    "Read iteration #{} failed. Error: {}",
                    self.requests[&request_id],
                    error
                );
            }
            DataWriteCompleted { request_id, size: _ } => {
                log_info!(
                    self.ctx,
                    "Write iteration #{} completed. Elapsed time = {}",
                    self.requests[&request_id] - READ_ITERATIONS,
                    self.ctx.time() - self.start_time,
                );
            }
            DataWriteFailed { request_id, error } => {
                log_info!(
                    self.ctx,
                    "Write iteration #{} failed. Error: {}",
                    self.requests[&request_id],
                    error
                );
            }
        })
    }
}

fn main() {
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    let mut sim = Simulation::new(SEED);
    let root = sim.create_context("root");

    let mut simple_spec = DiskSpec::default();
    simple_spec
        .set_capacity(DISK_CAPACITY)
        .set_constant_read_bw(DISK_READ_BW)
        .set_constant_write_bw(DISK_WRITE_BW);

    let simple_disk = rc!(refcell!(Disk::new(simple_spec, sim.create_context(SIMPLE_DISK_NAME))));
    sim.add_handler(SIMPLE_DISK_NAME, simple_disk.clone());

    println!("Starting simulation with simple disk...");

    let client = rc!(refcell!(Client::new(
        simple_disk,
        sim.create_context(SIMPLE_CLIENT_NAME)
    )));
    root.emit_now(Start {}, sim.add_handler(SIMPLE_CLIENT_NAME, client));

    // Elapsed times in logs will be equal for all activities.
    sim.step_until_no_events();

    println!("Finished simple user");

    let mut advanced_spec = DiskSpec::default();
    advanced_spec
        .set_capacity(DISK_CAPACITY)
        // Using the constant throughput function for read operations,
        // so total throughput will not depend on operations count.
        .set_read_throughput_fn(make_constant_throughput_fn(DISK_READ_BW))
        // Using custom throughput function for write operations,
        // so total throughput will depend on operations count `n` as follows.
        .set_write_throughput_fn(boxed!(|n| {
            if n < 4 {
                DISK_WRITE_BW
            } else {
                DISK_WRITE_BW / 2.
            }
        }))
        // Using the uniformly randomized factor function for read operations,
        // so operation's throughput will be multiplied by a random factor from 0.9 to 1.1.
        .set_read_factor_fn(boxed!(make_uniform_factor_fn(0.9, 1.1)))
        // Using the empirical factor function for write operations,
        // so operation's throughput will be multiplied by a random factor
        // generated from the specified weighted points distribution.
        .set_write_factor_fn(boxed!(ExampleDiskFactorFn {}));

    let advanced_disk = rc!(refcell!(Disk::new(
        advanced_spec,
        sim.create_context(ADVANCED_DISK_NAME),
    )));
    sim.add_handler(ADVANCED_DISK_NAME, advanced_disk.clone());

    println!("Starting advanced user...");

    let advanced_user = rc!(refcell!(Client::new(
        advanced_disk,
        sim.create_context(ADVANCED_CLIENT_NAME)
    )));
    root.emit_now(Start {}, sim.add_handler(ADVANCED_CLIENT_NAME, advanced_user));

    // Elapsed times in logs will differ for same activities.
    sim.step_until_no_events();

    println!("Finished advanced user");
}
