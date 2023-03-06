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
    // Total number of nodes in each test
    #[clap(short, long = "nodes-count", default_value_t = 50)]
    nodes_count: usize,

    // Total number of stars for multistar test
    #[clap(short, long = "stars-count", default_value_t = 10)]
    stars_count: usize,

    // Total number of switches for fat-tree test
    #[clap(short, long = "switch-count", default_value_t = 3)]
    switch_count: usize,

    // Total number of routers for fat-tree test
    #[clap(short, long = "router-count", default_value_t = 10)]
    router_count: usize,
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
}

fn make_star_topology(size: usize) -> Topology {
    let mut topology = Topology::new();

    let switch_name = "switch".to_string();
    topology.add_node(&switch_name, 1000.0, 0.0);

    for i in 0..size {
        let host_name = format!("host_{}", i);
        topology.add_node(&host_name, 1000.0, 0.0);
        topology.add_link(&host_name, &switch_name, 0.2, 200.0);
    }

    topology
}

fn make_tree_topology(stars_count: usize, star_size: usize) -> Topology {
    let mut topology = Topology::new();

    let root_switch_name = "root_switch".to_string();
    topology.add_node(&root_switch_name, 1000.0, 0.0);

    for j in 0..stars_count {
        let switch_name = format!("switch_{}", j);
        topology.add_node(&switch_name, 1000.0, 0.0);
        topology.add_link(&root_switch_name, &switch_name, 0.2, 1000.0);

        for i in 0..star_size {
            let host_name = format!("host_{}_{}", j, i);
            topology.add_node(&host_name, 1000.0, 0.0);
            topology.add_link(&host_name, &switch_name, 0.2, 200.0);
        }
    }

    topology
}

fn make_cluster_topology(size: usize) -> Topology {
    let mut topology = Topology::new();

    for i in 0..size {
        let host_name = format!("host_{}", i);
        topology.add_node(&host_name, 1000.0, 0.0);
    }

    for i in 0..size {
        for j in 0..i {
            topology.add_link(&format!("host_{}", i), &format!("host_{}", j), 0.2, 200.0);
        }
    }

    topology
}

fn make_fat_tree_topology(nodes_per_router: usize, switch_count: usize, router_count: usize) -> Topology {
    let mut topology = Topology::new();

    for i in 0..switch_count {
        let switch_name = format!("switch_{}", i);
        topology.add_node(&switch_name, 1000.0, 0.0);
    }

    for i in 0..router_count {
        let router_name = format!("router_{}", i);
        topology.add_node(&router_name, 1000.0, 0.0);

        for j in 0..nodes_per_router {
            let host_name = format!("host_{}_{}", i, j);
            topology.add_node(&host_name, 1000.0, 0.0);
            topology.add_link(&router_name, &host_name, 0.2, 200.0);
        }

        for j in 0..switch_count {
            topology.add_link(&router_name, &format!("switch_{}", j), 0.2, 1000.0);
        }
    }

    topology
}

fn init_topology(sim: &mut Simulation, topology: Topology) -> NetworkActors {
    let nodes = topology.get_nodes();

    let topology_rc = rc!(refcell!(topology));
    let network_model = rc!(refcell!(TopologyNetwork::new(topology_rc.clone())));
    let network = Network::new_with_topology(network_model, topology_rc.clone(), sim.create_context("net"));
    let network_rc = rc!(refcell!(network));
    sim.add_handler("net", network_rc.clone());

    let mut actors = NetworkActors::default();
    for host_name in nodes.into_iter() {
        if !host_name.starts_with("host_") {
            continue;
        }
        let sender_name = format!("sender_{}", &host_name[5..]);
        let receiver_name = format!("receiver_{}", &host_name[5..]);

        let sender = DataTransferRequester::new(network_rc.clone(), sim.create_context(&sender_name));
        let sender_id = sim.add_handler(sender_name, rc!(refcell!(sender)));
        actors.senders.push(sender_id);
        topology_rc.borrow_mut().set_location(sender_id, &host_name);

        let receiver = DataReceiver::new(network_rc.clone(), sim.create_context(&receiver_name));
        let receiver_id = sim.add_handler(receiver_name, rc!(refcell!(receiver)));
        actors.receivers.push(receiver_id);
        topology_rc.borrow_mut().set_location(receiver_id, &host_name);
    }
    topology_rc.borrow_mut().init();
    actors
}

fn run_benchmark(topology: Topology) {
    let mut sim = Simulation::new(SIMULATION_SEED);
    let actors = init_topology(&mut sim, topology);

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

    run_benchmark(make_tree_topology(
        args.nodes_count / args.stars_count,
        args.stars_count,
    ));
}

fn star_topology_benchmark(args: &Args) {
    println!("=== Star Benchmark ===");

    run_benchmark(make_star_topology(args.nodes_count));
}

fn cluster_topology_benchmark(args: &Args) {
    println!("=== Cluster Benchmark ===");

    run_benchmark(make_cluster_topology(args.nodes_count));
}

fn fat_tree_topology_benchmark(args: &Args) {
    println!("=== Fat-Tree Benchmark ===");

    run_benchmark(make_fat_tree_topology(
        args.nodes_count / args.router_count,
        args.switch_count,
        args.router_count,
    ));
}

fn main() {
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    let args = Args::parse();

    star_topology_benchmark(&args);
    multistar_topology_benchmark(&args);
    cluster_topology_benchmark(&args);
    fat_tree_topology_benchmark(&args);
}
