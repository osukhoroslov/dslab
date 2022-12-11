use std::cell::RefCell;
use std::collections::HashMap;
use std::io::Write;
use std::rc::Rc;

use dslab_storage::resource::StorageResource;
use env_logger::Builder;
use serde::Serialize;
use sugars::{rc, refcell};

use dslab_core::cast;
use dslab_core::context::SimulationContext;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;
use dslab_core::log_debug;
use dslab_core::simulation::Simulation;

use dslab_storage::events::{DataReadCompleted, DataReadFailed, DataWriteCompleted, DataWriteFailed};
use dslab_storage::shared_disk::SharedDisk;

const SEED: u64 = 16;

const DISK_NAME: &str = "SharedDisk";
const USER_NAME: &str = "User";

const DISK_CAPACITY: u64 = 1000;
const DISK_READ_BW: f64 = 100.;
const DISK_WRITE_BW: f64 = 100.;

struct User {
    disk: Rc<RefCell<SharedDisk>>,
    requests: HashMap<u64, u64>, // request_id -> test case
    ctx: SimulationContext,
}

#[derive(Serialize)]
struct Start {}

#[derive(Serialize)]
struct Ticker {}

impl User {
    fn new(disk: Rc<RefCell<SharedDisk>>, ctx: SimulationContext) -> Self {
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
            Start {} => {
                self.ctx.emit_self(Ticker {}, 0.);
            }
            Ticker {} => {
                let time = self.ctx.time();
                if time.eq(&0.) {
                    log_debug!(self.ctx, "Test #0: Single 100 byte read, expected to end at t=1");
                    self.requests.insert(self.disk.borrow_mut().read(100, self.ctx.id()), 0);
                } else if time.eq(&1.) {
                    log_debug!(self.ctx, "Test #1: Two 50 byte reads, expected to end at t=2");
                    self.requests.insert(self.disk.borrow_mut().read(50, self.ctx.id()), 1);
                    self.requests.insert(self.disk.borrow_mut().read(50, self.ctx.id()), 1);
                } else if time.eq(&2.) {
                    log_debug!(self.ctx, "Test #2: Starting 1st 200 byte read, expected to end at t=5");
                    self.requests.insert(self.disk.borrow_mut().read(200, self.ctx.id()), 2);
                } else if time.eq(&3.) {
                    log_debug!(self.ctx, "Test #2: Starting 2nd 200 byte read, expected to end at t=6");
                    self.requests.insert(self.disk.borrow_mut().read(200, self.ctx.id()), 2);
                    return;
                }
                self.ctx.emit_self(Ticker {}, 1.);
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

    let disk = rc!(refcell!(SharedDisk::new_simple(
        DISK_CAPACITY,
        DISK_READ_BW,
        DISK_WRITE_BW,
        sim.create_context(DISK_NAME),
    )));
    sim.add_handler(DISK_NAME, disk.clone());

    let user = rc!(refcell!(User::new(disk, sim.create_context(USER_NAME))));
    let user_id = sim.add_handler(USER_NAME, user);

    let mut root = sim.create_context("root");
    root.emit_now(Start {}, user_id);

    sim.step_until_no_events();

    println!("Finish");
}
