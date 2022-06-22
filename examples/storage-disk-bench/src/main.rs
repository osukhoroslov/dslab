mod random;

use std::cell::RefCell;
use std::io::Write;
use std::rc::Rc;
use std::time::Instant;

use env_logger::Builder;
use random::CustomRandom;
use serde::Serialize;
use sugars::{rc, refcell};

use dslab_core::context::SimulationContext;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;
use dslab_core::simulation::Simulation;
use dslab_core::{cast, log_debug, log_error, log_info};

use dslab_storage::events::{DataReadCompleted, DataReadFailed};
use dslab_storage::shared_disk::SharedDisk;

const SEED: u64 = 16;

const DISK_NAME: &str = "Disk";
const USER_NAME: &str = "User";

const DISK_CAPACITY: u64 = 10u64.pow(10);
const DISK_READ_BW: f64 = 100.;
const DISK_WRITE_BW: f64 = 100.;

const ACTIVITIES_COUNT: u64 = 10000;

struct User {
    disk: Rc<RefCell<SharedDisk>>,
    ctx: SimulationContext,
    activities_count: u64,
}

#[derive(Serialize)]
struct Start {
    activities_count: u64,
}

impl User {
    fn new(disk: Rc<RefCell<SharedDisk>>, ctx: SimulationContext) -> Self {
        Self {
            disk,
            ctx,
            activities_count: 0,
        }
    }
}

impl EventHandler for User {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            Start { activities_count } => {
                log_info!(self.ctx, "Starting disk benchmark");
                self.activities_count = activities_count;
                let mut rnd = CustomRandom::new(SEED);
                for _ in 0..activities_count {
                    self.disk.borrow_mut().read(rnd.next(), self.ctx.id());
                }
                log_info!(
                    self.ctx,
                    "Started {} activities. Waiting for complete...",
                    activities_count
                );
            }
            DataReadCompleted { request_id: _, size } => {
                self.activities_count -= 1;
                log_debug!(self.ctx, "Completed reading size = {}", size,);
                if self.activities_count == 0 {
                    log_info!(self.ctx, "Done.");
                }
            }
            DataReadFailed { request_id: _, error } => {
                log_error!(self.ctx, "Unexpected error: {}", error);
            }
        })
    }
}

fn main() {
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
    root.emit_now(
        Start {
            activities_count: ACTIVITIES_COUNT,
        },
        user_id,
    );

    let t = Instant::now();
    sim.step_until_no_events();
    let elapsed = t.elapsed().as_millis();
    println!("Processed {} iterations in {} ms", ACTIVITIES_COUNT, elapsed,);

    println!("Finish");
}
