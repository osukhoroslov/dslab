mod process;

use std::collections::HashSet;
use std::io::Write;
use std::time::Instant;

use clap::{arg, command};
use env_logger::Builder;
use sugars::{rc, refcell};

use simcore::simulation::Simulation;

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
            arg!([ASYMMETRIC])
                .help("Asymmetric mode")
                .validator(|s| s.parse::<u8>())
                .default_value("0"),
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
    let asymmetric = matches.value_of_t::<u8>("ASYMMETRIC").unwrap() > 0;
    let iterations = matches.value_of_t("ITERATIONS").unwrap();

    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    let mut sim = Simulation::new(123);
    let mut root = sim.create_context("root");

    for proc_id in 1..=proc_count {
        let proc_name = format!("proc{}", proc_id);
        let mut peers = HashSet::new();
        if peer_count == 1 {
            let peer_id = (proc_id % proc_count) + 1;
            peers.insert(peer_id);
        } else {
            while peers.len() < peer_count {
                let peer_id = root.gen_range(1..=proc_count);
                if peer_id != proc_id {
                    peers.insert(peer_id);
                }
            }
        }
        let is_pinger = !asymmetric || proc_id % 2 == 1;
        let proc = Process::new(
            Vec::from_iter(peers),
            is_pinger,
            iterations,
            sim.create_context(&proc_name),
        );
        sim.add_handler(&proc_name, rc!(refcell!(proc)));
        root.emit(Start {}, proc_id, 0.);
    }

    let t = Instant::now();
    sim.step_until_no_events();
    let elapsed = t.elapsed().as_secs_f64();
    println!(
        "Processed {} iterations / {} events in {:.2?} s ({:.0} events/s, {:.2} iter/s)",
        iterations,
        sim.event_count(),
        elapsed,
        sim.event_count() as f64 / elapsed,
        iterations as f64 / elapsed
    );
}
