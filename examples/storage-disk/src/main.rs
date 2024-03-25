use std::cell::RefCell;
use std::collections::HashMap;
use std::io::Write;
use std::rc::Rc;

use env_logger::Builder;
use log::LevelFilter;
use serde::Serialize;
use sugars::{rc, refcell};

use dslab_core::context::SimulationContext;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;
use dslab_core::simulation::Simulation;
use dslab_core::{cast, Id};
use dslab_core::{log_error, log_info};

use dslab_storage::disk::{Disk, DiskBuilder};
use dslab_storage::events::{DataReadCompleted, DataReadFailed, DataWriteCompleted, DataWriteFailed};
use dslab_storage::storage::Storage;

const SEED: u64 = 123;
const DISK_CAPACITY: u64 = 300;
const DISK_READ_BW: f64 = 150.;
const DISK_WRITE_BW: f64 = 125.;
const DISK_NAME: &str = "Disk";
const CLIENT_NAME: &str = "Client";

fn main() {
    // Setup logging
    Builder::new()
        .filter_level(LevelFilter::Info)
        .parse_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    // Build a simulation with a disk and a client
    let sim = Simulation::new(SEED);

    let disk = rc!(refcell!(DiskBuilder::simple(
        DISK_CAPACITY,
        DISK_READ_BW,
        DISK_WRITE_BW,
    )
    .build(sim.create_context(DISK_NAME))));
    sim.add_handler(DISK_NAME, disk.clone());

    let client = rc!(refcell!(DiskClient::new(disk, sim.create_context(CLIENT_NAME))));
    sim.add_handler(CLIENT_NAME, client.clone());

    // Run the simulation
    client.borrow_mut().start();
    sim.step_until_no_events();
}

struct DiskClient {
    disk: Rc<RefCell<Disk>>,
    requests: HashMap<u64, u64>, // request_id -> step
    ctx: SimulationContext,
}

#[derive(Clone, Serialize)]
struct Step {}

impl DiskClient {
    fn new(disk: Rc<RefCell<Disk>>, ctx: SimulationContext) -> Self {
        Self {
            disk,
            requests: HashMap::new(),
            ctx,
        }
    }

    fn id(&self) -> Id {
        self.ctx.id()
    }

    fn start(&mut self) {
        self.ctx.emit_self(Step {}, 0.);
    }

    fn print_disk_info(&self) {
        let disk_info = self.disk.borrow().info();
        log_info!(
            self.ctx,
            "Disk info: capacity = {}, used space = {}, free space = {}",
            disk_info.capacity,
            disk_info.used_space,
            disk_info.free_space
        )
    }
}

impl EventHandler for DiskClient {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            Step {} => {
                let time = self.ctx.time();
                let step = time as u64;
                match step {
                    0 => {
                        self.print_disk_info();
                        log_info!(self.ctx, "Step 0: Single 100 byte write, expected to end at t=0.8");
                        let req = self.disk.borrow_mut().write(100, self.id());
                        self.requests.insert(req, step);
                    }
                    1 => {
                        log_info!(
                            self.ctx,
                            "Step 1: Two concurrent 50 byte writes, expected to end at t=1.8"
                        );
                        let req1 = self.disk.borrow_mut().write(50, self.id());
                        let req2 = self.disk.borrow_mut().write(50, self.id());
                        self.requests.insert(req1, step);
                        self.requests.insert(req2, step);
                    }
                    2 => {
                        log_info!(
                            self.ctx,
                            "Step 2: Starting first 200 byte read, expected to end at t=3.667"
                        );
                        let req = self.disk.borrow_mut().read(200, self.id());
                        self.requests.insert(req, step);
                    }
                    3 => {
                        log_info!(
                            self.ctx,
                            "Step 3: Starting second 200 byte read, expected to end at t=4.667"
                        );
                        let req = self.disk.borrow_mut().read(200, self.id());
                        self.requests.insert(req, step);
                    }
                    4 => {
                        log_info!(self.ctx, "Step 4: Trying to write 101 bytes... should fail");
                        let req = self.disk.borrow_mut().write(101, self.id());
                        self.requests.insert(req, step);
                    }
                    5 => {
                        log_info!(self.ctx, "Step 5: Trying to read 301 bytes... should fail");
                        let req = self.disk.borrow_mut().read(301, self.id());
                        self.requests.insert(req, step);
                    }
                    6 => {
                        log_info!(
                            self.ctx,
                            "Step 6: Freeing space and trying to write once more... now success"
                        );
                        self.disk.borrow_mut().mark_free(1).unwrap();
                        self.print_disk_info();
                        let req = self.disk.borrow_mut().write(101, self.id());
                        self.requests.insert(req, step);
                        return;
                    }
                    _ => {}
                }
                self.ctx.emit_self(Step {}, 1.);
            }
            DataReadCompleted { request_id, size } => {
                log_info!(
                    self.ctx,
                    "Step {}: Completed reading {} bytes from disk",
                    self.requests[&request_id],
                    size
                );
                self.print_disk_info();
            }
            DataReadFailed { request_id, error } => {
                log_error!(
                    self.ctx,
                    "Step {}: Reading failed. Error: {}",
                    self.requests[&request_id],
                    error
                );
            }
            DataWriteCompleted { request_id, size } => {
                log_info!(
                    self.ctx,
                    "Step {}: Completed writing {} bytes to disk",
                    self.requests[&request_id],
                    size
                );
                self.print_disk_info();
            }
            DataWriteFailed { request_id, error } => {
                log_error!(
                    self.ctx,
                    "Step {}: Writing failed. Error: {}",
                    self.requests[&request_id],
                    error
                );
            }
        })
    }
}
