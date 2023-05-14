mod process;

use std::collections::BTreeSet;
use std::io::Write;
use std::time::Instant;

use clap::Parser;
use env_logger::Builder;
use sugars::{rc, refcell};

use dslab_core::Simulation;
use dslab_network::{
    constant_bandwidth_model::ConstantBandwidthNetwork, network::Network,
    shared_bandwidth_model::SharedBandwidthNetwork,
};

use crate::process::{Process, StartMessage};
use process::NetworkProcess;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Number of processes (>= 2)
    #[clap(long, default_value_t = 2)]
    proc_count: u32,

    /// Number of process peers (>= 1)
    #[clap(long, default_value_t = 1)]
    peer_count: usize,

    /// Asymmetric mode
    #[clap(long)]
    asymmetric: bool,

    /// Random delay
    #[clap(long)]
    rand_delay: bool,

    /// Use network
    #[clap(long)]
    use_network: bool,

    /// Number of iterations (>= 1)
    #[clap(long, default_value_t = 1)]
    iterations: u32,
}

fn main() {
    let args = Args::parse();
    let proc_count = args.proc_count;
    let iterations = args.iterations;

    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    let mut sim = Simulation::new(42);
    let root = sim.create_context("root");

    let network_opt = if args.use_network {
        let network_model = rc!(refcell!(ConstantBandwidthNetwork::new(1000., 0.001)));
        let network = rc!(refcell!(Network::new(network_model, sim.create_context("net"))));
        sim.add_handler("net", network.clone());

        network
            .borrow_mut()
            .add_node("host1", Box::new(SharedBandwidthNetwork::new(1000., 0.)));
        network
            .borrow_mut()
            .add_node("host2", Box::new(SharedBandwidthNetwork::new(1000., 0.)));
        Some(network)
    } else {
        None
    };

    let id_offset = args.use_network as u32;

    for i in 1..=proc_count {
        let proc_id = i + id_offset;
        let proc_name = format!("proc{}", i);
        let mut peers = BTreeSet::new();
        if args.peer_count == 1 {
            let peer_id = (i % proc_count) + 1 + id_offset;
            peers.insert(peer_id);
        } else {
            while peers.len() < args.peer_count {
                let peer_id = root.gen_range(1..=proc_count) + id_offset;
                if peer_id != proc_id {
                    peers.insert(peer_id);
                }
            }
        }
        let is_pinger = !args.asymmetric || i % 2 == 1;
        if let Some(ref network) = network_opt {
            let proc = NetworkProcess::new(
                Vec::from_iter(peers),
                is_pinger,
                iterations,
                network.clone(),
                sim.create_context(&proc_name),
            );
            sim.add_handler(&proc_name, rc!(refcell!(proc)));
            let host = format!("host{}", 2 - i % 2);
            network.borrow_mut().set_location(proc_id, &host);
        } else {
            let proc = Process::new(
                Vec::from_iter(peers),
                is_pinger,
                args.rand_delay,
                iterations,
                sim.create_context(&proc_name),
            );
            sim.add_handler(&proc_name, rc!(refcell!(proc)));
        }
        root.emit(StartMessage {}, proc_id, 0.);
    }

    let t = Instant::now();
    sim.step_until_no_events();

    let elapsed = t.elapsed().as_secs_f64();
    println!(
        "Processed {} iterations in {:.2?}s ({:.2} iter/s)",
        iterations,
        elapsed,
        iterations as f64 / elapsed
    );
    println!(
        "Processed {} events in {:.2?}s ({:.0} events/s)",
        sim.event_count(),
        elapsed,
        sim.event_count() as f64 / elapsed
    );
}
