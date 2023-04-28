use std::cell::RefCell;
use std::collections::HashMap;
use std::io::Write;
use std::rc::Rc;

use env_logger::Builder;
use serde::Serialize;
use sugars::{boxed, rc, refcell};

use dslab_core::cast;
use dslab_core::context::SimulationContext;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;
use dslab_core::log_info;
use dslab_core::simulation::Simulation;

use dslab_models::throughput_sharing::{make_constant_throughput_fn, make_uniform_factor_fn, EmpiricalFactorFn};
use dslab_storage::disk::{Disk, DiskSpec};
use dslab_storage::events::{DataReadCompleted, DataReadFailed, DataWriteCompleted, DataWriteFailed};
use dslab_storage::storage::Storage;

const SEED: u64 = 16;

const SIMPLE_DISK_NAME: &str = "SimpleDisk";
const ADVANCED_DISK_NAME: &str = "AdvancedDisk";

const SIMPLE_USER_NAME: &str = "SimpleUser";
const ADVANCED_USER_NAME: &str = "AdvancedUser";

const DISK_CAPACITY: u64 = 1000;
const DISK_READ_BW: f64 = 100.;
const DISK_WRITE_BW: f64 = 100.;

const READ_ITERATIONS: u64 = 100;
const WRITE_ITERATIONS: u64 = 100;

struct User {
    disk: Rc<RefCell<Disk>>,
    requests: HashMap<u64, u64>, // request_id -> test case
    ctx: SimulationContext,
    start_time: f64,
}

#[derive(Clone, Serialize)]
struct Start {}

#[derive(Clone, Serialize)]
struct Ticker {}

impl User {
    fn new(disk: Rc<RefCell<Disk>>, ctx: SimulationContext) -> Self {
        Self {
            disk,
            requests: HashMap::new(),
            ctx,
            start_time: 0.,
        }
    }
}

impl EventHandler for User {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            Start {} => {
                self.start_time = self.ctx.time();
                for i in 1..READ_ITERATIONS {
                    self.requests.insert(self.disk.borrow_mut().read(10, self.ctx.id()), i);
                }
                for i in 1..WRITE_ITERATIONS {
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

    let simple_spec = DiskSpec::default()
        .set_capacity(DISK_CAPACITY)
        .set_constant_read_bw(DISK_READ_BW)
        .set_constant_write_bw(DISK_WRITE_BW);

    let simple_disk = rc!(refcell!(Disk::new(simple_spec, sim.create_context(SIMPLE_DISK_NAME))));
    sim.add_handler(SIMPLE_DISK_NAME, simple_disk.clone());

    println!("Starting simple user...");

    let simple_user = rc!(refcell!(User::new(simple_disk, sim.create_context(SIMPLE_USER_NAME))));
    root.emit_now(Start {}, sim.add_handler(SIMPLE_USER_NAME, simple_user));

    // Elapsed times in logs will be equal for all activities.
    sim.step_until_no_events();

    println!("Finished simple user");

    let advanced_spec = DiskSpec::default()
        .set_capacity(DISK_CAPACITY)
        //
        // Using the constant throughput function for read operations,
        // so total throughput will not change during the simulation.
        .set_read_throughput_fn(make_constant_throughput_fn(DISK_READ_BW))
        //
        // Using custom throughput function for write operations,
        // so total throughput will depend on activities count `n` as follows.
        .set_write_throughput_fn(boxed!(|n| {
            if n < 4 {
                DISK_WRITE_BW
            } else {
                DISK_WRITE_BW / 2.
            }
        }))
        //
        // Using the uniformly randomized factor function for read operations,
        // so read bandwidth will be multiplied by random factor from 0.9 to 1.1.
        .set_read_factor_fn(boxed!(make_uniform_factor_fn(0.9, 1.1)))
        //
        // Using the empirical factor function for write operations,
        // so write bandwidth will be multiplied by factor generated from distribution weighted with given weights.
        .set_write_factor_fn(boxed!(EmpiricalFactorFn::new(&[
            (0.8, 3),
            (0.9, 10),
            (1., 31),
            (1.1, 15),
            (1.2, 5),
            (1.3, 6)
        ])
        .unwrap()));

    let advanced_disk = rc!(refcell!(Disk::new(
        advanced_spec,
        sim.create_context(ADVANCED_DISK_NAME),
    )));
    sim.add_handler(ADVANCED_DISK_NAME, advanced_disk.clone());

    println!("Starting advanced user...");

    let advanced_user = rc!(refcell!(User::new(
        advanced_disk,
        sim.create_context(ADVANCED_USER_NAME)
    )));
    root.emit_now(Start {}, sim.add_handler(ADVANCED_USER_NAME, advanced_user));

    // Elapsed times in logs will differ for same activities.
    sim.step_until_no_events();

    println!("Finished advanced user");
}
