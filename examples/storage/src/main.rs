use std::cell::RefCell;
use std::rc::Rc;

use sugars::{rc, refcell};

use rand::prelude::*;
use rand_pcg::Pcg64;

use core::actor::{Actor, ActorContext, ActorId, Event};
use core::cast;
use core::sim::Simulation;

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

struct UserActor {
    file_system: Rc<RefCell<FileSystem>>,
}

#[derive(Debug)]
struct Start {}

#[derive(Debug)]
struct Init {}

impl UserActor {
    fn new(file_system: Rc<RefCell<FileSystem>>) -> Self {
        Self { file_system }
    }
}

impl Actor for UserActor {
    fn on(&mut self, event: Box<dyn Event>, _from: ActorId, ctx: &mut ActorContext) {
        cast!(match event {
            Init {} => {
                self.file_system.borrow_mut().create(FILE_1_NAME);
                self.file_system.borrow_mut().create(FILE_2_NAME);

                ctx.emit_now(Start {}, ActorId::from(USER_NAME));
            }
            Start {} => {
                let mut rand = Pcg64::seed_from_u64(SEED);

                for _ in 1..ITER_COUNT {
                    let mut size = rand.gen_range(1..MAX_SIZE);
                    self.file_system.borrow_mut().read(FILE_1_NAME, size, ctx);

                    size = rand.gen_range(1..MAX_SIZE);
                    self.file_system.borrow_mut().write(FILE_1_NAME, size, ctx);

                    self.file_system.borrow_mut().read_all(FILE_2_NAME, ctx);

                    size = rand.gen_range(1..MAX_SIZE);
                    self.file_system.borrow_mut().write(FILE_2_NAME, size, ctx);
                }
            }
            FileReadCompleted { file_name, read_size } => {
                println!(
                    "{} [{}] completed READ {} bytes from file {}",
                    ctx.time(),
                    ctx.id,
                    read_size,
                    file_name
                );
            }
            FileWriteCompleted { file_name, new_size } => {
                println!(
                    "{} [{}] completed WRITE to file {}, new_size {}",
                    ctx.time(),
                    ctx.id,
                    file_name,
                    new_size
                );
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}

fn main() {
    println!("Starting...");

    let mut sim = Simulation::new(SEED);

    let disk1 = rc!(refcell!(Disk::new(
        DISK_1_NAME,
        DISK_1_CAPACITY,
        DISK_1_READ_BW,
        DISK_1_WRITE_BW,
    )));
    sim.add_actor(DISK_1_NAME, disk1);

    let disk2 = rc!(refcell!(Disk::new(
        DISK_2_NAME,
        DISK_2_CAPACITY,
        DISK_2_READ_BW,
        DISK_2_WRITE_BW,
    )));
    sim.add_actor(DISK_1_NAME, disk2);

    let file_system = rc!(refcell!(FileSystem::new(FILESYSTEM_NAME)));
    sim.add_actor(FILESYSTEM_NAME, file_system.clone());

    file_system
        .borrow_mut()
        .mount(DISK_1_MOUNT_POINT, ActorId::from(DISK_1_NAME));
    file_system
        .borrow_mut()
        .mount(DISK_2_MOUNT_POINT, ActorId::from(DISK_2_NAME));

    let user = rc!(refcell!(UserActor::new(file_system)));
    sim.add_actor(USER_NAME, user);

    sim.add_event_now(Init {}, ActorId::from(USER_NAME), ActorId::from(USER_NAME));

    sim.step_until_no_events();

    println!("Finish");
}
