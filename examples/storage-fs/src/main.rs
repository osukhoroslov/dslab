use std::cell::RefCell;
use std::rc::Rc;

use serde::Serialize;
use sugars::{rc, refcell};

use core::cast;
use core::context::SimulationContext;
use core::event::Event;
use core::handler::EventHandler;
use core::simulation::Simulation;

use storage::api::{FileReadCompleted, FileWriteCompleted};
use storage::disk::Disk;
use storage::file::FileSystem;

const SEED: u64 = 16;
const ITER_COUNT: u64 = 1000;
const MAX_SIZE: u64 = 1000;

const FILESYSTEM_NAME: &str = "FileSystem-1";
const DISK_1_NAME: &str = "Disk-1";
const DISK_2_NAME: &str = "Disk-2";

const USER_NAME: &str = "User";

const FILE_1_NAME: &str = "/disk1/file1";
const FILE_2_NAME: &str = "/disk2/file2";

const DISK_1_CAPACITY: u64 = 1000000000;
const DISK_2_CAPACITY: u64 = 10000000;

const DISK_1_MOUNT_POINT: &str = "/disk1/";
const DISK_2_MOUNT_POINT: &str = "/disk2/";

const DISK_1_READ_BW: u64 = 100;
const DISK_2_READ_BW: u64 = 100000;

const DISK_1_WRITE_BW: u64 = 100;
const DISK_2_WRITE_BW: u64 = 1000;

struct User {
    file_system: Rc<RefCell<FileSystem>>,
    ctx: SimulationContext,
}

#[derive(Serialize)]
struct Start {}

#[derive(Serialize)]
struct Init {}

impl User {
    fn new(file_system: Rc<RefCell<FileSystem>>, ctx: SimulationContext) -> Self {
        Self { file_system, ctx }
    }
}

impl EventHandler for User {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            Init {} => {
                self.file_system.borrow_mut().create_file(FILE_1_NAME);
                self.file_system.borrow_mut().create_file(FILE_2_NAME);

                self.ctx.emit_now(Start {}, USER_NAME);
            }
            Start {} => {
                for _ in 1..ITER_COUNT {
                    let size = (self.ctx.rand() * MAX_SIZE as f64) as u64;
                    self.file_system.borrow_mut().read(FILE_1_NAME, size, self.ctx.id());

                    let size = (self.ctx.rand() * MAX_SIZE as f64) as u64;
                    self.file_system.borrow_mut().write(FILE_1_NAME, size, self.ctx.id());

                    self.file_system.borrow_mut().read_all(FILE_2_NAME, self.ctx.id());

                    let size = (self.ctx.rand() * MAX_SIZE as f64) as u64;
                    self.file_system.borrow_mut().write(FILE_2_NAME, size, self.ctx.id());
                }
            }
            FileReadCompleted { file_name, read_size } => {
                println!(
                    "{} [{}] completed READ {} bytes from file {}",
                    self.ctx.time(),
                    self.ctx.id(),
                    read_size,
                    file_name
                );

                println!(
                    "{} [{}] total used space is {}",
                    self.ctx.time(),
                    self.ctx.id(),
                    self.file_system.borrow().get_used_space()
                );
            }
            FileWriteCompleted { file_name, new_size } => {
                println!(
                    "{} [{}] completed WRITE to file {}, new_size {}",
                    self.ctx.time(),
                    self.ctx.id(),
                    file_name,
                    new_size
                );

                println!(
                    "{} [{}] total used space is {}",
                    self.ctx.time(),
                    self.ctx.id(),
                    self.file_system.borrow().get_used_space()
                );
            }
        })
    }
}

fn main() {
    println!("Starting...");

    let mut sim = Simulation::new(SEED);

    let disk1 = rc!(refcell!(Disk::new(
        DISK_1_CAPACITY,
        DISK_1_READ_BW,
        DISK_1_WRITE_BW,
        sim.create_context(DISK_1_NAME),
    )));

    let disk2 = rc!(refcell!(Disk::new(
        DISK_2_CAPACITY,
        DISK_2_READ_BW,
        DISK_2_WRITE_BW,
        sim.create_context(DISK_2_NAME),
    )));

    let file_system = rc!(refcell!(FileSystem::new(sim.create_context(FILESYSTEM_NAME))));
    sim.add_handler(FILESYSTEM_NAME, file_system.clone());

    assert!(!file_system.borrow_mut().mount_disk(DISK_1_MOUNT_POINT, disk1));
    assert!(!file_system.borrow_mut().mount_disk(DISK_2_MOUNT_POINT, disk2));

    let user = rc!(refcell!(User::new(file_system, sim.create_context(USER_NAME))));
    sim.add_handler(USER_NAME, user);

    let mut root = sim.create_context("root");
    root.emit_now(Init {}, USER_NAME);

    sim.step_until_no_events();

    println!("Finish");
}
