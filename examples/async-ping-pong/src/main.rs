use std::{cell::RefCell, rc::Rc};

use process::StartMessage;
use sugars::{rc, refcell};

use async_dslab_core::{async_context::AsyncSimulationContext, async_simulation::AsyncSimulation};
mod process;

fn main() {
    let mut simulation = AsyncSimulation::new(42);

    let pinger_name = "pinger";
    let ponger_name = "ponger";

    let pinger_context = simulation.create_context(pinger_name);
    let ponget_context = simulation.create_context(ponger_name);

    let mut root_context = simulation.create_context("root");

    let pinger_id = pinger_context.id();
    let ponger_id = ponget_context.id();
    let pinger_process = process::Process {
        ctx: pinger_context,
        peers: Rc::new(RefCell::new(vec![ponger_id])),
        is_pinger: true,
    };

    let ponger_process = process::Process {
        ctx: ponget_context,
        peers: Rc::new(RefCell::new(vec![pinger_id])),
        is_pinger: false,
    };

    simulation.spawn(process::start_ponger(ponger_process));

    simulation.add_handler(&pinger_name, rc!(refcell!(pinger_process)));

    root_context.emit(
        StartMessage {
            content: "start_pinger_content".to_string(),
        },
        pinger_id,
        0.,
    );

    simulation.step_until_no_events();
}
