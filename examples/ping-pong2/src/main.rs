mod process;

use clap::{app_from_crate, arg};
use std::time::Instant;
use sugars::{rc, refcell};

use crate::process::{Process, Start};
use core2::simulator::Simulator;

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

    let mut sim = Simulator::new(123);

    let proc1 = Process::new(iterations, sim.create_context("proc1"));
    let proc2 = Process::new(iterations, sim.create_context("proc2"));
    sim.add_handler("proc1", rc!(refcell!(proc1)));
    sim.add_handler("proc2", rc!(refcell!(proc2)));

    let mut root = sim.create_context("root");
    root.emit(Start::new("proc2"), "proc1", 0.);
    root.emit(Start::new("proc1"), "proc2", 0.);

    let t = Instant::now();
    sim.step_until_no_events();
    println!(
        "Processed {} events in {:.2?} ({:.0} events/sec)",
        sim.event_count(),
        t.elapsed(),
        sim.event_count() as f64 / t.elapsed().as_secs_f64()
    );
}
