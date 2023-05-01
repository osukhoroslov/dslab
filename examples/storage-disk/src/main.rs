use std::cell::RefCell;
use std::collections::HashMap;
use std::io::Write;
use std::rc::Rc;

use env_logger::Builder;
use serde::Serialize;
use sugars::{rc, refcell};

use dslab_core::cast;
use dslab_core::context::SimulationContext;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;
use dslab_core::log_info;
use dslab_core::simulation::Simulation;

use dslab_storage::disk::Disk;
use dslab_storage::events::{DataReadCompleted, DataReadFailed, DataWriteCompleted, DataWriteFailed};
use dslab_storage::storage::Storage;

const SEED: u64 = 16;

const DISK_NAME: &str = "Disk";
const CLIENT_NAME: &str = "Client";

const DISK_CAPACITY: u64 = 300;
const DISK_READ_BW: f64 = 100.;
const DISK_WRITE_BW: f64 = 100.;

struct Client {
    disk: Rc<RefCell<Disk>>,
    requests: HashMap<u64, u64>, // request_id -> test case
    ctx: SimulationContext,
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
        }
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

impl EventHandler for Client {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            Start {} => {
                self.ctx.emit_self(Ticker {}, 0.);
            }
            Ticker {} => {
                let time = self.ctx.time();
                let step = time as u64;
                if time == 0. {
                    self.print_disk_info();
                    log_info!(self.ctx, "Step #0: Single 100 byte write, expected to end at t=1");
                    self.requests
                        .insert(self.disk.borrow_mut().write(100, self.ctx.id()), step);
                } else if time.eq(&1.) {
                    log_info!(self.ctx, "Step #1: Two 50 byte writes, expected to end at t=2");
                    self.requests.insert(self.disk.borrow_mut().write(50, self.ctx.id()), step);
                    self.requests.insert(self.disk.borrow_mut().write(50, self.ctx.id()), step);
                } else if time.eq(&2.) {
                    log_info!(self.ctx, "Step #2: Starting 1st 200 byte read, expected to end at t=5");
                    self.requests.insert(self.disk.borrow_mut().read(200, self.ctx.id()), step);
                } else if time.eq(&3.) {
                    log_info!(self.ctx, "Step #3: Starting 2nd 200 byte read, expected to end at t=6");
                    self.requests.insert(self.disk.borrow_mut().read(200, self.ctx.id()), step);
                } else if time.eq(&4.) {
                    log_info!(self.ctx, "Step #4: Trying to write 101 bytes... should fail");
                    self.requests
                        .insert(self.disk.borrow_mut().write(101, self.ctx.id()), step);
                } else if time.eq(&5.) {
                    log_info!(self.ctx, "Step #5: Trying to read 301 bytes... should fail");
                    self.requests.insert(self.disk.borrow_mut().read(301, self.ctx.id()), step);
                } else if time.eq(&6.) {
                    log_info!(
                        self.ctx,
                        "Step #6: Cleaning some space and trying to write once more... now success"
                    );
                    self.disk.borrow_mut().mark_free(1).unwrap();
                    self.print_disk_info();
                    self.requests
                        .insert(self.disk.borrow_mut().write(101, self.ctx.id()), step);
                    return;
                }
                self.ctx.emit_self(Ticker {}, 1.);
            }
            DataReadCompleted { request_id, size } => {
                log_info!(
                    self.ctx,
                    "Step #{}: Completed reading {} bytes from disk",
                    self.requests[&request_id],
                    size
                );
                self.print_disk_info();
            }
            DataReadFailed { request_id, error } => {
                log_info!(
                    self.ctx,
                    "Step #{}: Reading failed. Error: {}",
                    self.requests[&request_id],
                    error
                );
            }
            DataWriteCompleted { request_id, size } => {
                log_info!(
                    self.ctx,
                    "Step #{}: Completed writing {} bytes to disk",
                    self.requests[&request_id],
                    size
                );
                self.print_disk_info();
            }
            DataWriteFailed { request_id, error } => {
                log_info!(
                    self.ctx,
                    "Step #{}: Writing failed. Error: {}",
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

    let disk = rc!(refcell!(Disk::simple(
        DISK_CAPACITY,
        DISK_READ_BW,
        DISK_WRITE_BW,
        sim.create_context(DISK_NAME),
    )));
    sim.add_handler(DISK_NAME, disk.clone());

    let user = rc!(refcell!(Client::new(disk, sim.create_context(CLIENT_NAME))));
    let root = sim.create_context("root");

    println!("Starting...");
    root.emit_now(Start {}, sim.add_handler(CLIENT_NAME, user));
    sim.step_until_no_events();
    println!("Finished");
}
