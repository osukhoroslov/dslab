use std::cell::RefCell;
use std::rc::Rc;

use serde::Serialize;
use sugars::{rc, refcell};

use core::cast;
use core::context::SimulationContext;
use core::event::Event;
use core::handler::EventHandler;
use core::simulation::Simulation;

use storage::api::{DataReadCompleted, DataWriteCompleted};
use storage::disk::Disk;

const SEED: u64 = 16;
const ITER_COUNT: u64 = 1000;
const MAX_SIZE: u64 = 1000;

const DISK_NAME: &str = "Disk";
const USER_NAME: &str = "User";

const DISK_CAPACITY: u64 = 1000000000;
const DISK_READ_BW: u64 = 100;
const DISK_WRITE_BW: u64 = 100;

struct User {
    disk: Rc<RefCell<Disk>>,
    ctx: SimulationContext,
}

#[derive(Serialize)]
struct Start {}

impl User {
    fn new(disk: Rc<RefCell<Disk>>, ctx: SimulationContext) -> Self {
        Self { disk, ctx }
    }
}

impl EventHandler for User {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            Start {} => {
                for _ in 0..ITER_COUNT {
                    let size = (self.ctx.rand() * MAX_SIZE as f64) as u64;
                    self.disk.borrow_mut().read(size, self.ctx.id());

                    let size = (self.ctx.rand() * MAX_SIZE as f64) as u64;
                    self.disk.borrow_mut().write(size, self.ctx.id());
                }
            }
            DataReadCompleted { request_id: _, size } => {
                println!(
                    "{} [{}] completed READ {} bytes from disk",
                    self.ctx.time(),
                    self.ctx.id(),
                    size
                );
            }
            DataWriteCompleted { request_id: _, size } => {
                println!(
                    "{} [{}] completed WRITE {} bytes from disk",
                    self.ctx.time(),
                    self.ctx.id(),
                    size
                );
            }
        })
    }
}

fn main() {
    println!("Starting...");

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
