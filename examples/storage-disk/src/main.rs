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
use dslab_core::log_debug;
use dslab_core::simulation::Simulation;

use dslab_models::throughput_sharing::{make_constant_throughput_fn, make_uniform_factor_fn, EmpiricalFactorFn};
use dslab_storage::disk::Disk;
use dslab_storage::events::{DataReadCompleted, DataReadFailed, DataWriteCompleted, DataWriteFailed};
use dslab_storage::storage::Storage;

const SEED: u64 = 16;

const DISK_NAME: &str = "Disk";
const USER_NAME: &str = "User";

const DISK_CAPACITY: u64 = 1000;
const DISK_READ_BW: f64 = 100.;
const DISK_WRITE_BW: f64 = 100.;

struct User {
    disk: Rc<RefCell<Disk>>,
    requests: HashMap<u64, u64>, // request_id -> test case
    ctx: SimulationContext,
}

#[derive(Clone, Serialize)]
struct Start1 {}

#[derive(Clone, Serialize)]
struct Start2 {}

#[derive(Clone, Serialize)]
struct Ticker {}

impl User {
    fn new(disk: Rc<RefCell<Disk>>, ctx: SimulationContext) -> Self {
        Self {
            disk,
            requests: HashMap::new(),
            ctx,
        }
    }
}

impl EventHandler for User {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            Start1 {} => {
                self.ctx.emit_self(Ticker {}, 0.);
            }
            Ticker {} => {
                let time = self.ctx.time();
                if time.eq(&0.) {
                    log_debug!(self.ctx, "Test #1.0: Single 100 byte read, expected to end at t=1");
                    self.requests.insert(self.disk.borrow_mut().read(100, self.ctx.id()), 0);
                } else if time.eq(&1.) {
                    log_debug!(self.ctx, "Test #1.1: Two 50 byte reads, expected to end at t=2");
                    self.requests.insert(self.disk.borrow_mut().read(50, self.ctx.id()), 1);
                    self.requests.insert(self.disk.borrow_mut().read(50, self.ctx.id()), 1);
                } else if time.eq(&2.) {
                    log_debug!(
                        self.ctx,
                        "Test #1.2: Starting 1st 200 byte read, expected to end at t=5"
                    );
                    self.requests.insert(self.disk.borrow_mut().read(200, self.ctx.id()), 2);
                } else if time.eq(&3.) {
                    log_debug!(
                        self.ctx,
                        "Test #1.2: Starting 2nd 200 byte read, expected to end at t=6"
                    );
                    self.requests.insert(self.disk.borrow_mut().read(200, self.ctx.id()), 2);
                    return;
                }
                self.ctx.emit_self(Ticker {}, 1.);
            }
            Start2 {} => {
                log_debug!(self.ctx, "Test #2.0: Reading 600 bytes... should be OK");
                self.requests.insert(self.disk.borrow_mut().read(600, self.ctx.id()), 0);

                log_debug!(self.ctx, "Test #2.1: Then trying to read 1200 bytes... should fail");
                self.requests
                    .insert(self.disk.borrow_mut().read(1200, self.ctx.id()), 1);

                log_debug!(self.ctx, "Used space: {}", self.disk.borrow().used_space());

                log_debug!(self.ctx, "Test #2.2: Writing 800 bytes... should be OK");
                self.requests
                    .insert(self.disk.borrow_mut().write(800, self.ctx.id()), 2);

                log_debug!(self.ctx, "Used space: {}", self.disk.borrow().used_space());

                log_debug!(self.ctx, "Test #2.3: Writing 400 more bytes... should fail");
                self.requests
                    .insert(self.disk.borrow_mut().write(400, self.ctx.id()), 3);

                log_debug!(self.ctx, "Used space: {}", self.disk.borrow().used_space());
            }
            DataReadCompleted { request_id, size } => {
                log_debug!(
                    self.ctx,
                    "Test #{}: Completed reading {} bytes from disk",
                    self.requests[&request_id],
                    size
                );
            }
            DataReadFailed { request_id, error } => {
                log_debug!(
                    self.ctx,
                    "Test #{}: Reading failed. Error: {}",
                    self.requests[&request_id],
                    error
                );
            }
            DataWriteCompleted { request_id, size } => {
                log_debug!(
                    self.ctx,
                    "Test #{}: Completed writing {} bytes to disk",
                    self.requests[&request_id],
                    size
                );
            }
            DataWriteFailed { request_id, error } => {
                log_debug!(
                    self.ctx,
                    "Test #{}: Writing failed. Error: {}",
                    self.requests[&request_id],
                    error
                );
            }
        })
    }
}

fn main() {
    println!("Starting...");

    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    let mut sim = Simulation::new(SEED);

    // Creating empirical factor function with weighted points distribution
    let model = EmpiricalFactorFn::new(&[(0.8, 3), (0.9, 10), (1., 31), (1.1, 15), (1.2, 5), (1.3, 6)]);
    assert!(model.is_ok());

    let disk = rc!(refcell!(Disk::new(
        DISK_CAPACITY,
        // Using the constant throughput function for read operations
        make_constant_throughput_fn(DISK_READ_BW),
        make_constant_throughput_fn(DISK_WRITE_BW),
        // Using the created factor function for read operations
        boxed!(model.unwrap()),
        // Using the randomized uniform factor function for write operations
        boxed!(make_uniform_factor_fn(0.9, 1.1)),
        sim.create_context(DISK_NAME),
    )));
    sim.add_handler(DISK_NAME, disk.clone());

    let user = rc!(refcell!(User::new(disk.clone(), sim.create_context(USER_NAME))));
    let user_id = sim.add_handler(USER_NAME, user);

    let root = sim.create_context("root");
    root.emit_now(Start {}, user_id);

    println!("Starting test pack 1...");
    root.emit_now(Start1 {}, user_id);
    sim.step_until_no_events();

    println!("Clearing disk after test pack 1...");
    let used_space = disk.borrow().used_space();
    assert!(disk.borrow_mut().mark_free(used_space).is_ok());

    println!("Starting test pack 2...");
    root.emit_now(Start2 {}, user_id);
    sim.step_until_no_events();

    println!("Finish");
}
