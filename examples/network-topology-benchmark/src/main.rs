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

/// Network topology benchmarks
#[derive(Parser, Debug)]
#[clap(about, long_about = None)]
struct Args {
    /// Total number of hosts in topology
    #[clap(long = "hosts", default_value_t = 64)]
    host_count: usize,

    /// Number of stars in tree topology
    #[clap(long = "stars", default_value_t = 8)]
    star_count: usize,

    /// Number of level-1 (bottom) switches in fat-tree topology
    #[clap(long = "l1-switches", default_value_t = 8)]
    l1_switch_count: usize,

    /// Number of level-2 (top) switches in fat-tree topology
    #[clap(long = "l2-switches", default_value_t = 4)]
    l2_switch_count: usize,
}

#[derive(Serialize)]
pub struct Start {
    size: f64,
    receiver_id: Id,
}

pub struct DataSender {
    net: Rc<RefCell<Network>>,
    ctx: SimulationContext,
}

impl DataSender {
    pub fn new(net: Rc<RefCell<Network>>, ctx: SimulationContext) -> Self {
        Self { net, ctx }
    }
}

impl EventHandler for DataSender {
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

fn make_star_topology(host_count: usize) -> Topology {
    let mut topology = Topology::new();

    let switch_name = "switch".to_string();
    topology.add_node(&switch_name, 1000.0, 0.0);

    for i in 0..host_count {
        let host_name = format!("host_{}", i);
        topology.add_node(&host_name, 1000.0, 0.0);
        topology.add_link(&host_name, &switch_name, 200.0, 0.2);
    }

    topology.init();
    topology
}

fn make_tree_topology(star_count: usize, hosts_per_star: usize) -> Topology {
    let mut topology = Topology::new();

    let root_switch_name = "root_switch".to_string();
    topology.add_node(&root_switch_name, 1000.0, 0.0);

    for i in 0..star_count {
        let switch_name = format!("switch_{}", i);
        topology.add_node(&switch_name, 1000.0, 0.0);
        topology.add_link(&root_switch_name, &switch_name, 1000.0, 0.2);

        for j in 0..hosts_per_star {
            let host_name = format!("host_{}_{}", i, j);
            topology.add_node(&host_name, 1000.0, 0.0);
            topology.add_link(&host_name, &switch_name, 200.0, 0.2);
        }
    }

    topology.init();
    topology
}

fn make_fat_tree_topology(l2_switch_count: usize, l1_switch_count: usize, hosts_per_switch: usize) -> Topology {
    let mut topology = Topology::new();

    for i in 0..l2_switch_count {
        let switch_name = format!("l2_switch_{}", i);
        topology.add_node(&switch_name, 1000.0, 0.0);
    }

    let downlink_bw = 200.;
    let uplink_bw = downlink_bw * hosts_per_switch as f64 / l2_switch_count as f64;

    for i in 0..l1_switch_count {
        let switch_name = format!("l1_switch_{}", i);
        topology.add_node(&switch_name, 1000.0, 0.0);

        for j in 0..hosts_per_switch {
            let host_name = format!("host_{}_{}", i, j);
            topology.add_node(&host_name, 1000.0, 0.0);
            topology.add_link(&switch_name, &host_name, downlink_bw, 0.2);
        }

        for j in 0..l2_switch_count {
            topology.add_link(&switch_name, &format!("l2_switch_{}", j), uplink_bw, 0.2);
        }
    }

    topology.init();
    topology
}

fn make_full_mesh_topology(host_count: usize) -> Topology {
    let mut topology = Topology::new();

    for i in 0..host_count {
        let host_name = format!("host_{}", i);
        topology.add_node(&host_name, 1000.0, 0.0);
    }

    for i in 0..host_count {
        for j in 0..i {
            topology.add_link(&format!("host_{}", i), &format!("host_{}", j), 200.0, 0.2);
        }
    }

    topology.init();
    topology
}

fn init_network(sim: &mut Simulation, topology: Topology) -> NetworkActors {
    let topology_rc = rc!(refcell!(topology));
    let network_model = rc!(refcell!(TopologyNetwork::new(topology_rc.clone())));
    let network = Network::new_with_topology(network_model, topology_rc, sim.create_context("net"));
    let network_rc = rc!(refcell!(network));
    sim.add_handler("net", network_rc.clone());

    let mut actors = NetworkActors::default();
    let nodes = network_rc.borrow().get_nodes();
    for node_name in nodes {
        if !node_name.starts_with("host_") {
            continue;
        }
        let sender_name = format!("sender_{}", &node_name[5..]);
        let receiver_name = format!("receiver_{}", &node_name[5..]);

        let sender = DataSender::new(network_rc.clone(), sim.create_context(&sender_name));
        let sender_id = sim.add_handler(sender_name, rc!(refcell!(sender)));
        actors.senders.push(sender_id);
        network_rc.borrow_mut().set_location(sender_id, &node_name);

        let receiver = DataReceiver::new(network_rc.clone(), sim.create_context(&receiver_name));
        let receiver_id = sim.add_handler(receiver_name, rc!(refcell!(receiver)));
        actors.receivers.push(receiver_id);
        network_rc.borrow_mut().set_location(receiver_id, &node_name);
    }
    actors
}

fn run_benchmark(topology: Topology) {
    let mut sim = Simulation::new(SIMULATION_SEED);
    let actors = init_network(&mut sim, topology);

    let mut client = sim.create_context("client");

    for sender in actors.senders {
        for &receiver in &actors.receivers {
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
        "Processed {} events in {:.2?} ({:.0} events/sec)\n",
        sim.event_count(),
        now.elapsed(),
        sim.event_count() as f64 / now.elapsed().as_secs_f64()
    );
}

fn star_topology_benchmark(args: &Args) {
    println!("=== Star Topology ===");
    run_benchmark(make_star_topology(args.host_count));
}

fn tree_topology_benchmark(args: &Args) {
    println!("=== Tree Topology ===");
    run_benchmark(make_tree_topology(args.star_count, args.host_count / args.star_count));
}

fn fat_tree_topology_benchmark(args: &Args) {
    println!("=== Fat-Tree Topology ===");
    run_benchmark(make_fat_tree_topology(
        args.l2_switch_count,
        args.l1_switch_count,
        args.host_count / args.l1_switch_count,
    ));
}

fn full_mesh_topology_benchmark(args: &Args) {
    println!("=== Full Mesh Topology ===");
    run_benchmark(make_full_mesh_topology(args.host_count));
}

fn main() {
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    let args = Args::parse();

    star_topology_benchmark(&args);
    tree_topology_benchmark(&args);
    fat_tree_topology_benchmark(&args);
    full_mesh_topology_benchmark(&args);
}
