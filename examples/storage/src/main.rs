use log::info;

use std::cell::RefCell;
use std::rc::Rc;

use storage::api::{IOReadCompleted, IOReadRequest, IOWriteCompleted, IOWriteRequest};
use storage::block_device::BlockDevice;
use storage::scheduler::{NoOpIOScheduler, ScanIOScheduler};

use core::actor::{Actor, ActorContext, ActorId, Event};
use core::cast;
use core::sim::Simulation;

extern crate env_logger;

#[derive(Debug, Clone)]
pub struct Start {}

pub struct IOUserActor {
    io_driver: ActorId,
}

impl IOUserActor {
    pub fn new(io_driver: &str) -> Self {
        Self {
            io_driver: ActorId::from(io_driver),
        }
    }
}

impl Actor for IOUserActor {
    fn on(&mut self, event: Box<dyn Event>, from: ActorId, ctx: &mut ActorContext) {
        cast!(match event {
            Start {} => {
                info!("Starting...");
                ctx.emit_now(IOReadRequest { start: 0, count: 100 }, self.io_driver.clone());
                ctx.emit_now(
                    IOWriteRequest {
                        start: 123456,
                        count: 789,
                    },
                    self.io_driver.clone(),
                );
            }
            IOReadCompleted { src_event_id } => {
                info!(
                    "{} [{}] received IOReadCompleted for request {} from {}",
                    ctx.time(),
                    ctx.id,
                    src_event_id,
                    from
                );
            }
            IOWriteCompleted { src_event_id } => {
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

    // create disk and driver for it

    let disk = Rc::new(RefCell::new(BlockDevice::new("hdd_disk".to_string(), 1.0, 1.0, 1)));

    // sim.add_actor("io_driver", Rc::new(RefCell::new(ScanIOScheduler::new(ActorId::from("io_driver"), disk, 1.0))));
    sim.add_actor("io_driver", Rc::new(RefCell::new(NoOpIOScheduler::new(disk))));

    // create disk user

    sim.add_actor("user", Rc::new(RefCell::new(IOUserActor::new("io_driver"))));

    // start the simulation

    sim.add_event(Start {}, ActorId::from("0"), ActorId::from("user"), 0.);

    sim.step_until_no_events();

    info!("Finish");
}
