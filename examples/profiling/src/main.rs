#![doc = include_str!("../readme.md")]

pub mod components;

use std::time::Instant;

use clap::Parser;
use sugars::{rc, refcell};

use dslab_core::Simulation;

use components::{Client, Server};

/// Profiling example
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Number of clients (>= 1)
    #[clap(long, default_value_t = 10)]
    clients_count: u32,

    /// Choose next client to send event randomly
    #[clap(long)]
    rand_clients_choose: bool,

    /// Use emit_ordered to improve performance
    #[clap(long)]
    use_emit_ordered: bool,

    /// Number of events (>= 1)
    #[clap(long, default_value_t = 100)]
    events_count: u64,

    /// Display messages count
    #[clap(long)]
    display_messages_count: bool,
}

fn main() {
    let args = Args::parse();

    let mut sim = Simulation::new(123);

    let mut clients = vec![];
    let mut clients_ids = vec![];

    for number in 0..args.clients_count {
        let client = rc!(refcell!(Client::default()));
        clients_ids.push(sim.add_handler(format!("client_{}", number), client.clone()));
        clients.push(client);
    }

    let server = Server::new(
        sim.create_context("server"),
        clients_ids,
        args.events_count,
        args.use_emit_ordered,
        args.rand_clients_choose,
    );

    let t = Instant::now();

    server.start();
    sim.step_until_no_events();

    let elapsed = t.elapsed().as_secs_f64();

    if args.display_messages_count {
        for (i, client) in clients.iter().enumerate() {
            println!(
                "Messages received by client {}: {}",
                i,
                client.borrow().messages_count()
            );
        }
    }

    println!(
        "Processed {} events in {:.2?}s ({:.0} events/s)",
        sim.event_count(),
        elapsed,
        sim.event_count() as f64 / elapsed
    );
}
