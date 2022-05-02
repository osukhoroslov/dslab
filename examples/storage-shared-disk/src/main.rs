use std::cell::RefCell;
use std::collections::HashMap;
use std::io::Write;
use std::rc::Rc;

use env_logger::Builder;
use serde::Serialize;
use sugars::{rc, refcell};

use simcore::cast;
use simcore::context::SimulationContext;
use simcore::event::Event;
use simcore::handler::EventHandler;
use simcore::log_debug;
use simcore::simulation::Simulation;

use storage::events::{DataReadCompleted, DataReadFailed, DataWriteCompleted, DataWriteFailed};
use storage::shared_disk::SharedDisk;

const SEED: u64 = 16;

const DISK_NAME: &str = "SharedDisk";
const USER_NAME: &str = "User";

const DISK_CAPACITY: u64 = 1000;
const DISK_READ_BW: u64 = 100;
const DISK_WRITE_BW: u64 = 100;

struct User {
    disk: Rc<RefCell<SharedDisk>>,
    requests: HashMap<u64, u64>, // request_id -> test case
    ctx: SimulationContext,
}

#[derive(Serialize)]
struct Start {}

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
                log_debug!(self.ctx, "Test #0: Reading 200 bytes...");
                self.requests.insert(self.disk.borrow_mut().read(200, self.ctx.id()), 0);

                log_debug!(self.ctx, "Test #1: Read 400 bytes more...");
                self.requests.insert(self.disk.borrow_mut().read(400, self.ctx.id()), 1);
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

    let disk = rc!(refcell!(SharedDisk::new(
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
