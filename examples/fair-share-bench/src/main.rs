use clap::Parser;
use env_logger::Builder;
use std::cell::RefCell;
use std::io::Write;
use std::rc::Rc;
use std::time::Instant;

use serde::Serialize;
use sugars::{rc, refcell};

use dslab_compute::singlecore::*;
use dslab_core::component::Id;
use dslab_core::context::SimulationContext;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;
use dslab_core::simulation::Simulation;
use dslab_core::{cast, log_error, log_info};

#[derive(Clone, Serialize)]
pub struct Start {}

struct Component {
    compute: Rc<RefCell<Compute>>,
    ctx: SimulationContext,
    computations_left: u32,
}

impl Component {
    pub fn new(compute: Rc<RefCell<Compute>>, ctx: SimulationContext, computations: u32) -> Self {
        Self {
            compute,
            ctx,
            computations_left: computations,
        }
    }

    pub fn start(&self, initial_computations: u32) {
        for _ in 0..initial_computations {
            self.compute.borrow_mut().run(self.ctx.rand() * 100., 0, self.ctx.id());
        }
    }
}

impl EventHandler for Component {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            CompStarted { id } => {
                log_info!(
                    self.ctx,
                    "received CompStarted from {} for {:?}",
                    self.ctx.lookup_name(event.src),
                    id
                );
            }
            CompFinished { id } => {
                log_info!(
                    self.ctx,
                    "received CompFinished from {} for {:?}",
                    self.ctx.lookup_name(event.src),
                    id
                );
                if self.computations_left > 0 {
                    self.computations_left -= 1;
                    self.compute.borrow_mut().run(self.ctx.rand() * 100., 0, self.ctx.id());
                }
            }
        });
    }
}

/// Ping-pong example (version using async mode)
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(long, default_value_t = 1)]
    initial: u32,

    #[clap(long, default_value_t = 1)]
    total: u32,
}

fn main() {
    let args = Args::parse();

    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    let mut sim = Simulation::new(123);

    let compute = rc!(refcell!(Compute::new(10., 1024, sim.create_context("compute"))));
    sim.add_handler("compute", compute.clone());

    let component = rc!(refcell!(Component::new(
        compute.clone(),
        sim.create_context("component"),
        args.total
    )));
    sim.add_handler("component", component.clone());

    component.borrow().start(args.initial);

    let t = Instant::now();

    sim.step_until_no_events();

    let elapsed = t.elapsed().as_secs_f64();
    println!("Elapsed time: {:?}s", elapsed);
    println!(
        "Processed events: {:.2} ({:.0}events/sec)",
        sim.event_count(),
        sim.event_count() as f64 / elapsed
    );
}
