use std::cell::RefCell;
use std::io::Write;
use std::rc::Rc;

use env_logger::Builder;
use log::LevelFilter;
use rand::distributions::Uniform;
use sugars::{boxed, rc, refcell};

use dslab_core::cast;
use dslab_core::context::SimulationContext;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;
use dslab_core::simulation::Simulation;
use dslab_core::{log_error, log_info};

use dslab_models::throughput_sharing::{make_constant_throughput_fn, make_uniform_factor_fn, ActivityFactorFn};
use dslab_storage::disk::{Disk, DiskBuilder, DiskOperation};
use dslab_storage::events::{DataReadCompleted, DataReadFailed, DataWriteCompleted, DataWriteFailed};
use dslab_storage::storage::Storage;

const SEED: u64 = 12345;
const DISK_NAME: &str = "Disk";
const DISK_CAPACITY: u64 = 1000;
const DISK_READ_BW: f64 = 125.;
const DISK_WRITE_BW: f64 = 100.;
const CLIENT_NAME: &str = "Client";

fn main() {
    // Setup logging
    Builder::new()
        .filter_level(LevelFilter::Info)
        .parse_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    // Simple disk model
    let simple_disk_builder = DiskBuilder::new()
        .capacity(DISK_CAPACITY)
        .constant_read_bw(DISK_READ_BW)
        .constant_write_bw(DISK_WRITE_BW);

    // Advanced disk model
    let advanced_disk_builder = DiskBuilder::new()
        .capacity(DISK_CAPACITY)
        // Using the constant throughput function for read operations,
        // so total throughput will not depend on operations count.
        .read_throughput_fn(make_constant_throughput_fn(DISK_READ_BW))
        // Using custom throughput function for write operations,
        // so total throughput will depend on operations count `n` as follows.
        .write_throughput_fn(boxed!(|n| {
            if n <= 4 {
                DISK_WRITE_BW
            } else {
                DISK_WRITE_BW / 2.
            }
        }))
        // Using the uniformly randomized factor function for read operations,
        // so operation's throughput will be multiplied by a random factor from 0.8 to 1.1.
        .read_factor_fn(boxed!(make_uniform_factor_fn(0.8, 1.1)))
        // Using the custom factor function for write operations
        // with dependency on operation's data size and randomization.
        .write_factor_fn(boxed!(ExampleActivityFactorFn {}));

    println!("Simulation with simple disk model:");
    run_simulation(simple_disk_builder);

    println!("\nSimulation with advanced disk model:");
    run_simulation(advanced_disk_builder);
}

fn run_simulation(disk_builder: DiskBuilder) {
    let mut sim = Simulation::new(SEED);

    let disk = rc!(refcell!(disk_builder.build(sim.create_context(DISK_NAME))));
    sim.add_handler(DISK_NAME, disk.clone());

    let client = rc!(refcell!(DiskClient::new(disk, sim.create_context(CLIENT_NAME))));
    sim.add_handler(CLIENT_NAME, client.clone());

    client.borrow_mut().start();
    sim.step_until_no_events();
}

struct ExampleActivityFactorFn {}

impl ActivityFactorFn<DiskOperation> for ExampleActivityFactorFn {
    fn get_factor(&mut self, item: &DiskOperation, ctx: &SimulationContext) -> f64 {
        if item.size < 100 {
            1.
        } else {
            ctx.sample_from_distribution(&Uniform::<f64>::new(0.8, 1.))
        }
    }
}

struct DiskClient {
    disk: Rc<RefCell<Disk>>,
    ctx: SimulationContext,
}

impl DiskClient {
    fn new(disk: Rc<RefCell<Disk>>, ctx: SimulationContext) -> Self {
        Self { disk, ctx }
    }

    fn start(&mut self) {
        for _ in 0..6 {
            self.disk.borrow_mut().write(20, self.ctx.id());
        }
        for _ in 0..4 {
            self.disk.borrow_mut().write(180, self.ctx.id());
        }
        for _ in 0..10 {
            self.disk.borrow_mut().read(100, self.ctx.id());
        }
    }
}

impl EventHandler for DiskClient {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            DataReadCompleted { request_id, size: _ } => {
                log_info!(self.ctx, "Read {} completed", request_id - 10);
            }
            DataReadFailed { request_id, error } => {
                log_error!(self.ctx, "Read {} failed: {}", request_id - 10, error);
            }
            DataWriteCompleted { request_id, size: _ } => {
                log_info!(self.ctx, "Write {} completed", request_id,);
            }
            DataWriteFailed { request_id, error } => {
                log_error!(self.ctx, "Write {} failed: {}", request_id, error);
            }
        })
    }
}
