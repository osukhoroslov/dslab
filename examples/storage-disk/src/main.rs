use std::cell::RefCell;
use std::collections::HashMap;
use std::io::Write;
use std::rc::Rc;

use serde::Serialize;
use sugars::{rc, refcell};

use env_logger::Builder;

use core::cast;
use core::context::SimulationContext;
use core::event::Event;
use core::handler::EventHandler;
use core::log_debug;
use core::simulation::Simulation;

use storage::api::{DataReadCompleted, DataReadFailed, DataWriteCompleted, DataWriteFailed};
use storage::disk::Disk;

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

                log_debug!(
                    self.ctx,
                    "Test #1: Then trying to read 6 bytes... should fail"
                );
                self.requests.insert(self.disk.borrow_mut().read(6, self.ctx.id()), 1);

                log_debug!(self.ctx, "Used space: {}", self.disk.borrow_mut().get_used_space());

                log_debug!(self.ctx, "Test #2: Writing 4 bytes... should be OK");
                self.requests.insert(self.disk.borrow_mut().write(4, self.ctx.id()), 2);

                log_debug!(self.ctx, "Used space: {}", self.disk.borrow_mut().get_used_space());

                log_debug!(self.ctx, "Test #3: Writing 2 more bytes... should fail");
                self.requests.insert(self.disk.borrow_mut().write(2, self.ctx.id()), 3);

                log_debug!(self.ctx, "Used space: {}", self.disk.borrow_mut().get_used_space());
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

    let disk = rc!(refcell!(Disk::new(
        DISK_CAPACITY,
        DISK_READ_BW,
        DISK_WRITE_BW,
        sim.create_context(DISK_NAME),
    )));

    let user = rc!(refcell!(User::new(disk, sim.create_context(USER_NAME))));
    sim.add_handler(USER_NAME, user);

    let mut root = sim.create_context("root");
    root.emit_now(Start {}, USER_NAME);

    sim.step_until_no_events();

    println!("Finish");
}
