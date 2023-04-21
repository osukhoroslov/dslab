mod system;
mod topology;

use std::io::Write;

use clap::Parser;
use env_logger::Builder;
use std::time::Instant;

use dslab_core::Simulation;
use dslab_network::topology::Topology;

use system::{build_system, Start};
use topology::*;

const SIMULATION_SEED: u64 = 123;

/// Network topology benchmarks
#[derive(Parser, Debug)]
#[command(about, long_about = None)]
struct Args {
    /// Total number of hosts in topology
    #[arg(long = "hosts", default_value_t = 64)]
    host_count: usize,

    /// Number of stars in tree topology
    #[arg(long = "stars", default_value_t = 8)]
    star_count: usize,

    /// Number of level-1 (bottom) switches in fat-tree topology
    #[arg(long = "l1-switches", default_value_t = 8)]
    l1_switch_count: usize,

    /// Number of level-2 (top) switches in fat-tree topology
    #[arg(long = "l2-switches", default_value_t = 4)]
    l2_switch_count: usize,
}

fn main() {
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    let args = Args::parse();

    println!("=== Full Mesh Topology ===");
    run_benchmark(make_full_mesh_topology(args.host_count));

    println!("=== Star Topology ===");
    run_benchmark(make_star_topology(args.host_count));

    println!("=== Tree Topology ===");
    run_benchmark(make_tree_topology(args.star_count, args.host_count / args.star_count));

    println!("=== Fat-Tree Topology ===");
    run_benchmark(make_fat_tree_topology(
        args.l2_switch_count,
        args.l1_switch_count,
        args.host_count / args.l1_switch_count,
    ));
}

fn run_benchmark(topology: Topology) {
    let mut sim = Simulation::new(SIMULATION_SEED);
    let sys = build_system(&mut sim, topology);

    let client = sim.create_context("client");

    for sender in sys.senders {
        for &receiver in &sys.receivers {
            client.emit(
                Start {
                    data_size: sim.gen_range(1.0..1000.0),
                    receiver_id: receiver,
                },
                sender,
                sim.gen_range(0.0..10.0),
            );
        }
    }

    let now = Instant::now();
    sim.step_until_no_events();
    println!("Simulation time: {:.2}", sim.time());
    println!(
        "Processed {} events in {:.2?} ({:.0} events/sec)\n",
        sim.event_count(),
        now.elapsed(),
        sim.event_count() as f64 / now.elapsed().as_secs_f64()
    );
}
