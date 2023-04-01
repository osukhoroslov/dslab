use async_dslab_core::{async_context::AsyncSimulationContext, async_simulation::AsyncSimulation};
mod process;

fn main() {
    let mut simulation = AsyncSimulation::new(42);

    let pinger_context = simulation.create_context("pinger");
    let ponget_context = simulation.create_context("ponger");

    simulation.spawn(process::start_pinger(pinger_context));
    simulation.spawn(process::start_ponger(ponget_context));

    simulation.step_until_no_events();
}
