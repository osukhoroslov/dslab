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

use dslab_storage::bandwidth::{make_uniform_bw_model, EmpiricalBWModel};
use dslab_storage::disk::Disk;
use dslab_storage::events::{DataReadCompleted, DataReadFailed, DataWriteCompleted, DataWriteFailed};

const SEED: u64 = 16;

const DISK_NAME: &str = "Disk";
const USER_NAME: &str = "User";

const DISK_CAPACITY: u64 = 5;
const DISK_READ_BW: u64 = 100;
const DISK_WRITE_BW: u64 = 100;

struct User {
    disk: Rc<RefCell<Disk>>,
    requests: HashMap<u64, u64>, // request_id -> test case
    ctx: SimulationContext,
}

#[derive(Serialize)]
struct Start {}

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
            Start {} => {
                log_debug!(self.ctx, "Test #0: Reading 3 bytes... should be OK");
                self.requests.insert(self.disk.borrow_mut().read(3, self.ctx.id()), 0);

                log_debug!(self.ctx, "Test #1: Then trying to read 6 bytes... should fail");
                self.requests.insert(self.disk.borrow_mut().read(6, self.ctx.id()), 1);

                log_debug!(self.ctx, "Used space: {}", self.disk.borrow().used_space());

                log_debug!(self.ctx, "Test #2: Writing 4 bytes... should be OK");
                self.requests.insert(self.disk.borrow_mut().write(4, self.ctx.id()), 2);

                log_debug!(self.ctx, "Used space: {}", self.disk.borrow().used_space());

                log_debug!(self.ctx, "Test #3: Writing 2 more bytes... should fail");
                self.requests.insert(self.disk.borrow_mut().write(2, self.ctx.id()), 3);

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

    // Creating empirical bandwidth model with weighted points distribution
    let points = [
        (DISK_READ_BW - 20, 3),
        (DISK_READ_BW - 10, 10),
        (DISK_READ_BW, 31),
        (DISK_READ_BW + 10, 15),
        (DISK_READ_BW + 20, 5),
        (DISK_READ_BW + 30, 6),
    ];
    let model = EmpiricalBWModel::new(&points);
    assert!(model.is_ok());

    let disk = rc!(refcell!(Disk::new(
        DISK_CAPACITY,
        // Using created model as read bandwidth model for disk
        boxed!(model.unwrap()),
        // Creating randomized bandwidth model with uniform distribution in [DISK_WRITE_BW - 10; DISK_WRITE_BW + 10)
        boxed!(make_uniform_bw_model(DISK_WRITE_BW - 10, DISK_WRITE_BW + 10)),
        sim.create_context(DISK_NAME),
    )));

    let user = rc!(refcell!(User::new(disk, sim.create_context(USER_NAME))));
    let user_id = sim.add_handler(USER_NAME, user);

    let mut root = sim.create_context("root");
    root.emit_now(Start {}, user_id);

    sim.step_until_no_events();

    println!("Finish");
}
