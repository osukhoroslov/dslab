use std::borrow::BorrowMut;

use sugars::{rc, refcell};

use rand::prelude::*;
use rand_pcg::Pcg64;

use core::actor::{Actor, ActorContext, ActorId, Event};
use core::cast;
use core::sim::Simulation;

use storage::api::{DataReadCompleted, DataWriteCompleted};
use storage::disk::Disk;

pub const ITER_COUNT: u64 = 10;
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
            &DataReadCompleted { src_event_id: _, size } => {
                println!("{} [{}] completed READ {} bytes from disk", ctx.time(), ctx.id, size);
            }
            &DataWriteCompleted { src_event_id: _, size } => {
                println!("{} [{}] completed WRITE {} bytes from disk", ctx.time(), ctx.id, size);
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
    let disk_name = "disk-1";

    let mut sim = Simulation::new(seed);

    let disk = rc!(refcell!(Disk::new(disk_name, 1234, 4321)));
    sim.add_actor(disk_name, disk.clone());

    let user = rc!(refcell!(UserActor::new()));
    let user_actor_id = sim.add_actor("user", user);

    let mut rand = Pcg64::seed_from_u64(seed);

    for _ in 1..ITER_COUNT {
        let mut size = rand.gen_range(1..MAX_SIZE);
        (*disk)
            .borrow_mut()
            .read_async(size, sim.borrow_mut(), user_actor_id.clone());

        size = rand.gen_range(1..MAX_SIZE);
        (*disk)
            .borrow_mut()
            .write_async(size, sim.borrow_mut(), user_actor_id.clone());
    }

    sim.step_until_no_events();

    println!("Finish");
}
