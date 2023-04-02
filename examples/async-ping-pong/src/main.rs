use std::{cell::RefCell, rc::Rc};

use async_dslab_core::{async_context::AsyncSimulationContext, async_simulation::AsyncSimulation};
mod process;

fn main() {
    let mut simulation = AsyncSimulation::new(42);

    let pinger_context = simulation.create_context("pinger");
    let ponget_context = simulation.create_context("ponger");

    let pinger_id = pinger_context.id();
    let ponger_id = ponget_context.id();
    let pinger_process = process::Process {
        ctx: pinger_context,
        peers: Rc::new(RefCell::new(vec![ponger_id])),
    };

    let ponger_process = process::Process {
        ctx: ponget_context,
        peers: Rc::new(RefCell::new(vec![pinger_id])),
    };

    simulation.spawn(process::start_pinger(pinger_process));
    simulation.spawn(process::start_ponger(ponger_process));

    simulation.step_until_no_events();
}
