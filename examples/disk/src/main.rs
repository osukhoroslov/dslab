use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::rc::Rc;

use sugars::{rc, refcell};

use rand::prelude::*;
use rand_pcg::Pcg64;

use core::actor::{Actor, ActorContext, ActorId, Event};
use core::cast;
use core::sim::Simulation;

use storage::api::{DataReadCompleted, DataWriteCompleted};
use storage::disk::Disk;

const SEED: u64 = 16;
const ITER_COUNT: u64 = 1000;
const MAX_SIZE: u64 = 1000;

const DISK_NAME: &str = "Disk";
const USER_NAME: &str = "User";

struct UserActor {
    disk: Rc<RefCell<Disk>>,
}

#[derive(Debug)]
struct Start {}

impl UserActor {
    fn new(disk: Rc<RefCell<Disk>>) -> Self {
        Self { disk }
    }
}

impl Actor for UserActor {
    fn on(&mut self, event: Box<dyn Event>, _from: ActorId, ctx: &mut ActorContext) {
        cast!(match event {
            Start {} => {
                let mut rand = Pcg64::seed_from_u64(SEED);

                for _ in 1..ITER_COUNT {
                    let mut size = rand.gen_range(1..MAX_SIZE);
                    (*self.disk).borrow_mut().read(size, ctx.borrow_mut());

                    size = rand.gen_range(1..MAX_SIZE);
                    (*self.disk).borrow_mut().write(size, ctx.borrow_mut());
                }
            }
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

    let mut sim = Simulation::new(SEED);

    let disk = rc!(refcell!(Disk::new(DISK_NAME, 10000, 1234, 4321)));
    sim.add_actor(DISK_NAME, disk.clone());

    let user = rc!(refcell!(UserActor::new(disk)));
    sim.add_actor(USER_NAME, user);

    sim.add_event_now(Start {}, ActorId::from(USER_NAME), ActorId::from(USER_NAME));

    sim.step_until_no_events();

    println!("Finish");
}
