use rand::prelude::*;
use rand_pcg::Pcg64;
use std::borrow::BorrowMut;
use sugars::{rc, refcell};

use core::actor::{Actor, ActorContext, ActorId, Event};
use core::cast;
use core::sim::Simulation;

use storage::api::{FileReadCompleted, FileWriteCompleted};
use storage::disk::Disk;
use storage::file::{FileSystem, FS_ID};

pub const ITER_COUNT : u64 = 10;
pub const MAX_SIZE: u64 = 1000;

pub struct UserActor {}

impl UserActor {
    pub fn new() -> Self {
        Self {}
    }
}

impl Actor for UserActor {
    fn on(&mut self, event: Box<dyn Event>, _from: ActorId, ctx: &mut ActorContext) {
        cast!(match event {
            FileReadCompleted { fd } => {
                println!("{} [{}] completed READ from file {}", ctx.time(), ctx.id, fd,);
            }
            FileWriteCompleted { fd, new_size } => {
                println!(
                    "{} [{}] completed WRITE to file {}, new_size {}",
                    ctx.time(),
                    ctx.id,
                    fd,
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

    let seed = 16;

    let mut sim = Simulation::new(seed);

    let disk_name = "disk-1";

    let disk = rc!(refcell!(Disk::new(disk_name, 1234, 4321)));
    let disk_actor_id = sim.add_actor("disk-1", disk);

    let fs = rc!(refcell!(FileSystem::new(disk_actor_id)));
    sim.add_actor(FS_ID, fs.clone());

    let user = rc!(refcell!(UserActor::new()));
    let user_actor_id = sim.add_actor("user", user);

    let fd0 = (*fs).borrow_mut().open("/home/file0");
    let fd1 = (*fs).borrow_mut().open("/home/file1");

    let mut rand = Pcg64::seed_from_u64(seed);

    for _ in 1..ITER_COUNT {
        let mut size = rand.gen_range(1..MAX_SIZE);
        (*fs)
            .borrow_mut()
            .read_async(fd0, size, sim.borrow_mut(), user_actor_id.clone());

        size = rand.gen_range(1..MAX_SIZE);
        (*fs)
            .borrow_mut()
            .write_async(fd0, size, sim.borrow_mut(), user_actor_id.clone());

        size = rand.gen_range(1..MAX_SIZE);
        (*fs)
            .borrow_mut()
            .read_async(fd1, size, sim.borrow_mut(), user_actor_id.clone());

        size = rand.gen_range(1..MAX_SIZE);
        (*fs)
            .borrow_mut()
            .write_async(fd1, size, sim.borrow_mut(), user_actor_id.clone());
    }

    sim.step_until_no_events();

    println!("Finish");
}
