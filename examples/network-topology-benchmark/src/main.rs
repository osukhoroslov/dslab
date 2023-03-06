use std::cell::RefCell;
use std::io::Write;
use std::rc::Rc;
use std::time::Instant;

use clap::Parser;
use env_logger::Builder;
use serde::Serialize;
use sugars::{rc, refcell};

use dslab_core::component::Id;
use dslab_core::context::SimulationContext;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;
use dslab_core::simulation::Simulation;
use dslab_core::{cast, log_debug};
use dslab_network::model::{DataTransferCompleted, MessageDelivery};
use dslab_network::network::Network;
use dslab_network::topology::Topology;
use dslab_network::topology_model::TopologyNetwork;

const SIMULATION_SEED: u64 = 123;

#[derive(Parser, Debug)]
#[clap(about, long_about = None)]
struct Args {
    // Total number of nodes
    #[clap(short, long = "nodes-count", default_value_t = 50)]
    nodes_count: usize,

    // Total number of stars for multistar test
    #[clap(short, long = "stars-count", default_value_t = 10)]
    stars_count: usize,
}

#[derive(Serialize)]
pub struct Start {
    size: f64,
    receiver_id: Id,
}

pub struct DataTransferRequester {
    net: Rc<RefCell<Network>>,
    ctx: SimulationContext,
}

impl DataTransferRequester {
    pub fn new(net: Rc<RefCell<Network>>, ctx: SimulationContext) -> Self {
        Self { net, ctx }
    }
}

impl EventHandler for DataTransferRequester {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            Start { size, receiver_id } => {
                self.net
                    .borrow_mut()
                    .transfer_data(self.ctx.id(), receiver_id, size, receiver_id);
            }
            MessageDelivery { message: _message } => {
                log_debug!(self.ctx, "Sender: data transfer completed");
            }
        })
    }
}

pub struct DataReceiver {
    net: Rc<RefCell<Network>>,
    ctx: SimulationContext,
}

impl DataReceiver {
    pub fn new(net: Rc<RefCell<Network>>, ctx: SimulationContext) -> Self {
        Self { net, ctx }
    }
}

impl EventHandler for DataReceiver {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            DataTransferCompleted { data } => {
                self.net
                    .borrow_mut()
                    .send_msg("data transfer ack".to_string(), self.ctx.id(), data.src);
                log_debug!(self.ctx, "Receiver: data transfer completed");
            }
        })
    }
}

#[derive(Debug, Default)]
pub struct NetworkActors {
    pub receivers: Vec<u32>,
    pub senders: Vec<u32>,
    pub compute_nodes: Vec<String>,
    pub commutator_nodes: Vec<String>,
}

fn init_star_topology(sim: &mut Simulation, network: &Rc<RefCell<Network>>, size: usize) -> NetworkActors {
    let mut result = NetworkActors::default();

    let commutator_node_name = "commutator".to_string();
    result.commutator_nodes.push(commutator_node_name.clone());

    network.borrow_mut().add_node(&commutator_node_name, 1000.0, 0.0);

    for i in 0..size {
        let receiver_name = format!("receiver_{}", i);
        let sender_name = format!("sender_{}", i);
        let compute_node_name = format!("compute_node_{}", i);

        result.compute_nodes.push(compute_node_name.clone());
        network.borrow_mut().add_node(&compute_node_name, 1000.0, 0.0);

        network
            .borrow_mut()
            .add_link(&compute_node_name, &commutator_node_name, 0.2, 200.0);

        let sender = DataTransferRequester::new(network.clone(), sim.create_context(&sender_name));
        let sender_id = sim.add_handler(sender_name, rc!(refcell!(sender)));
        network.borrow_mut().set_location(sender_id, &compute_node_name);
        result.senders.push(sender_id);

        let receiver = DataReceiver::new(network.clone(), sim.create_context(&receiver_name));
        let receiver_id = sim.add_handler(receiver_name, rc!(refcell!(receiver)));
        network.borrow_mut().set_location(receiver_id, &compute_node_name);
        result.receivers.push(receiver_id);
    }

    let now = Instant::now();
    network.borrow_mut().init_topology();
    println!("Topology init time: {} ms", now.elapsed().as_millis());

    result
}

fn init_multi_star_topology(
    sim: &mut Simulation,
    network: &Rc<RefCell<Network>>,
    stars_count: usize,
    star_size: usize,
) -> NetworkActors {
    let mut result = NetworkActors::default();

    let center_commutator_node_name = "center_commutator".to_string();
    result.commutator_nodes.push(center_commutator_node_name.clone());

    network.borrow_mut().add_node(&center_commutator_node_name, 1000.0, 0.0);

    for j in 0..stars_count {
        let commutator_node_name = format!("commurator_{}", j);
        result.commutator_nodes.push(commutator_node_name.clone());

        network.borrow_mut().add_node(&commutator_node_name, 1000.0, 0.0);

        network
            .borrow_mut()
            .add_link(&center_commutator_node_name, &commutator_node_name, 0.2, 1000.0);

        for i in 0..star_size {
            let receiver_name = format!("receiver_{}", i);
            let sender_name = format!("sender_{}", i);
            let compute_node_name = format!("compute_node_{}", i);

            result.compute_nodes.push(compute_node_name.clone());
            network.borrow_mut().add_node(&compute_node_name, 1000.0, 0.0);

            network
                .borrow_mut()
                .add_link(&compute_node_name, &commutator_node_name, 0.2, 200.0);

            let sender = DataTransferRequester::new(network.clone(), sim.create_context(&sender_name));
            let sender_id = sim.add_handler(sender_name, rc!(refcell!(sender)));
            network.borrow_mut().set_location(sender_id, &compute_node_name);
            result.senders.push(sender_id);

            let receiver = DataReceiver::new(network.clone(), sim.create_context(&receiver_name));
            let receiver_id = sim.add_handler(receiver_name, rc!(refcell!(receiver)));
            network.borrow_mut().set_location(receiver_id, &compute_node_name);
            result.receivers.push(receiver_id);
        }
    }

    let now = Instant::now();
    network.borrow_mut().init_topology();
    println!("Topology init time: {} ms", now.elapsed().as_millis());

    result
}

fn start_benchmark(sim: &mut Simulation, actors: &NetworkActors) {
    let mut client = sim.create_context("client");

    for &sender in actors.senders.iter() {
        for &receiver in actors.receivers.iter() {
            client.emit(
                Start {
                    size: sim.gen_range(500.0..2000.0),
                    receiver_id: receiver,
                },
                sender,
                sim.gen_range(0.0..100.0),
            );
        }
    }

    let now = Instant::now();
    sim.step_until_no_events();
    println!("Simulation time: {:.2}", sim.time());
    println!(
        "Processed {} events in {:.2?} ({:.0} events/sec)",
        sim.event_count(),
        now.elapsed(),
        sim.event_count() as f64 / now.elapsed().as_secs_f64()
    );
}

fn multistar_topology_benchmark(args: &Args) {
    println!("=== Multistar Benchmark ===");

    let mut sim = Simulation::new(SIMULATION_SEED);

    let topology_rc = rc!(refcell!(Topology::new()));

    let topology_network_model = rc!(refcell!(TopologyNetwork::new(topology_rc.clone())));
    let topology_network = rc!(refcell!(Network::new_with_topology(
        topology_network_model,
        topology_rc,
        sim.create_context("net")
    )));
    sim.add_handler("net", topology_network.clone());

    let network_actors = init_multi_star_topology(
        &mut sim,
        &topology_network,
        args.nodes_count / args.stars_count,
        args.stars_count,
    );

    start_benchmark(&mut sim, &network_actors);
}

fn star_topology_benchmark(args: &Args) {
    println!("=== Star Benchmark ===");
    let mut sim = Simulation::new(SIMULATION_SEED);

    let topology_rc = rc!(refcell!(Topology::new()));
    let topology_network = rc!(refcell!(Network::new_with_topology(
        rc!(refcell!(TopologyNetwork::new(topology_rc.clone()))),
        topology_rc,
        sim.create_context("net")
    )));
    sim.add_handler("net", topology_network.clone());

    let network_actors = init_star_topology(&mut sim, &topology_network, args.nodes_count);

    start_benchmark(&mut sim, &network_actors);
}

fn main() {
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    let args = Args::parse();

    star_topology_benchmark(&args);
    multistar_topology_benchmark(&args);
}
