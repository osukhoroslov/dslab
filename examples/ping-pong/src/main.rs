use clap::{app_from_crate, arg};
use log::debug;
use std::time::Instant;
use sugars::{rc, refcell};

use core::actor::{Actor, ActorContext, ActorId, Event};
use core::match_event;
use core::sim::Simulation;

// EVENTS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct Start {
    other: ActorId,
}

#[derive(Debug)]
pub struct Ping {}

#[derive(Debug)]
pub struct Pong {}

// ACTORS //////////////////////////////////////////////////////////////////////////////////////////

pub struct Process {
    iterations: u32,
}

impl Process {
    pub fn new(iterations: u32) -> Self {
        Self { iterations }
    }
}

impl Actor for Process {
    fn on(&mut self, event: Box<dyn Event>, from: ActorId, ctx: &mut ActorContext) {
        match_event!( event {
            Start { other } => {
                debug!("{:.2} [{}] received Start from {}", ctx.time(), ctx.id, from);
                let delay = ctx.rand();
                ctx.emit(Ping {}, other.clone(), delay);
            },
            Ping {} => {
                debug!("{:.2} [{}] received Ping from {}", ctx.time(), ctx.id, from);
                let delay = ctx.rand();
                ctx.emit(Pong {}, from, delay);
            },
            Pong {} => {
                debug!("{:.2} [{}] received Pong from {}", ctx.time(), ctx.id, from);
                self.iterations -= 1;
                if self.iterations > 0 {
                    let delay = ctx.rand();
                    ctx.emit(Ping {}, from, delay);
                }
            },
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}

// MAIN ////////////////////////////////////////////////////////////////////////////////////////////

fn main() {
    let matches = app_from_crate!()
        .arg(
            arg!([ITERATIONS])
                .help("Number of iterations")
                .validator(|s| s.parse::<u64>())
                .default_value("1"),
        )
        .get_matches();
    let iterations = matches.value_of_t("ITERATIONS").unwrap();
    env_logger::init();

    let mut sim = Simulation::new(123);
    let proc1 = sim.add_actor("proc1", rc!(refcell!(Process::new(iterations))));
    let proc2 = sim.add_actor("proc2", rc!(refcell!(Process::new(iterations))));

    let root = ActorId::from("root");
    sim.add_event_now(Start { other: proc2.clone() }, root.clone(), proc1.clone());
    sim.add_event_now(Start { other: proc1.clone() }, root.clone(), proc2.clone());

    let t = Instant::now();
    sim.step_until_no_events();
    println!(
        "Processed {} events in {:.2?} ({:.0} events/sec)",
        sim.event_count(),
        t.elapsed(),
        sim.event_count() as f64 / t.elapsed().as_secs_f64()
    );
}
