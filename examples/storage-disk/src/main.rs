use sugars::{rc, refcell};
use serde::Serialize;

use core::cast;
use core::context::SimulationContext;
use core::event::Event;
use core::handler::EventHandler;
use core::simulation::Simulation;

use storage::api::{DataReadCompleted, DataReadRequest, DataWriteCompleted, DataWriteRequest};
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
    ctx: SimulationContext,
}

#[derive(Serialize)]
struct Start {}

impl User {
    fn new(ctx: SimulationContext) -> Self {
        Self { ctx }
    }
}

impl EventHandler for User {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            Start {} => {
                for _ in 0..ITER_COUNT {
                    let size = (self.ctx.rand() * MAX_SIZE as f64) as u64;
                    self.ctx.emit_now(DataReadRequest { size }, DISK_NAME);

                    let size = (self.ctx.rand() * MAX_SIZE as f64) as u64;
                    self.ctx.emit_now(DataWriteRequest { size }, DISK_NAME);
                }
            }
            DataReadCompleted { src_event_id: _, size } => {
                println!(
                    "{} [{}] completed READ {} bytes from disk",
                    self.ctx.time(),
                    self.ctx.id(),
                    size
                );
            }
            DataWriteCompleted { src_event_id: _, size } => {
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

    let disk = Disk::new(
        sim.create_context(DISK_NAME),
        DISK_CAPACITY,
        DISK_READ_BW,
        DISK_WRITE_BW,
    );
    sim.add_handler(DISK_NAME, rc!(refcell!(disk)));

    let mut user_ctx = sim.create_context(USER_NAME);
    user_ctx.emit_now(Start {}, USER_NAME);
    sim.add_handler(USER_NAME, rc!(refcell!(User::new(user_ctx))));

    sim.step_until_no_events();

    println!("Finish");
}
