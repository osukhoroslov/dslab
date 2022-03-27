mod process;

use std::collections::HashSet;
use std::io::Write;
use std::time::Instant;

use clap::{arg, command};
use env_logger::Builder;
use sugars::{rc, refcell};

use core::simulation::Simulation;

use crate::process::{Process, Start};

// MAIN ////////////////////////////////////////////////////////////////////////////////////////////

fn main() {
    let matches = command!()
        .arg(
            arg!([PROC_COUNT])
                .help("Number of processes (>= 2)")
                .validator(|s| s.parse::<u64>())
                .default_value("2"),
        )
        .arg(
            arg!([PEER_COUNT])
                .help("Number of process peers (>= 1)")
                .validator(|s| s.parse::<u64>())
                .default_value("1"),
        )
        .arg(
            arg!([ITERATIONS])
                .help("Number of iterations (>= 1)")
                .validator(|s| s.parse::<u64>())
                .default_value("1"),
        )
        .get_matches();
    let proc_count = matches.value_of_t("PROC_COUNT").unwrap();
    let peer_count = matches.value_of_t("PEER_COUNT").unwrap();
    let iterations = matches.value_of_t("ITERATIONS").unwrap();

    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    let mut sim = Simulation::new(123);
    let mut root = sim.create_context("root");

    for i in 0..proc_count {
        let proc_id = format!("proc{}", i);
        let mut peers = HashSet::new();
        while peers.len() < peer_count {
            let peer = root.gen_range(0..proc_count);
            if peer != i {
                peers.insert(format!("proc{}", peer));
            }
        }
        let proc = Process::new(Vec::from_iter(peers), iterations, sim.create_context(&proc_id));
        sim.add_handler(&proc_id, rc!(refcell!(proc)));
        root.emit(Start {}, &proc_id, 0.);
    }

    let t = Instant::now();
    sim.step_until_no_events();
    println!(
        "Processed {} events in {:.2?} ({:.0} events/sec)",
        sim.event_count(),
        t.elapsed(),
        sim.event_count() as f64 / t.elapsed().as_secs_f64()
    );
}
