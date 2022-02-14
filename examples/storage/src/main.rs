use log::info;

use sugars::{rc, refcell};

use core::actor::{Actor, ActorContext, ActorId, Event};
use core::cast;
use core::sim::Simulation;

use storage::api::{DataReadCompleted, DataReadRequest, DataWriteCompleted, DataWriteRequest};
use storage::disk::Disk;
use storage::file::{File, FileSystem};

extern crate env_logger;

#[derive(Debug, Clone)]
pub struct Start {}

pub struct IOUserActor {
    disk_actor_id: ActorId,
}

impl IOUserActor {
    pub fn new(disk_actor_id: ActorId) -> Self {
        Self { disk_actor_id }
    }
}

impl Actor for IOUserActor {
    fn on(&mut self, event: Box<dyn Event>, from: ActorId, ctx: &mut ActorContext) {
        cast!(match event {
            Start {} => {
                info!("Starting...");
                ctx.emit_now(DataReadRequest { size: 100 }, self.disk_actor_id.clone());
                ctx.emit_now(DataWriteRequest { size: 100 }, self.disk_actor_id.clone());
            }
            DataReadCompleted { src_event_id } => {
                info!(
                    "{} [{}] received IOReadCompleted for request {} from {}",
                    ctx.time(),
                    ctx.id,
                    src_event_id,
                    from
                );
            }
            DataWriteCompleted { src_event_id } => {
                info!(
                    "{} [{}] received IOWriteCompleted for request {} from {}",
                    ctx.time(),
                    ctx.id,
                    src_event_id,
                    from
                );
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}

fn main() {
    env_logger::init();

    let mut sim = Simulation::new(16);

    // create disk, fs and get file from it
    let disk = rc!(refcell!(Disk::new("disk1", 1234, 4321)));
    let mut fs = FileSystem::new(ActorId::from("disk1"));
    let file = fs.open("file1");

    file.seek(1);

    // create disk user
    sim.add_actor("user", rc!(refcell!(IOUserActor::new(ActorId::from("disk1")))));

    // start the simulation
    sim.add_event(Start {}, ActorId::from("0"), ActorId::from("user"), 0.);

    file.close();

    sim.step_until_no_events();

    info!("Finish");
}
