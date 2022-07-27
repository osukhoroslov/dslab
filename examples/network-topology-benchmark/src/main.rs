use std::cell::RefCell;
use std::io::Write;
use std::rc::Rc;
use std::time::Instant;

use dslab_network::topology::Topology;
use env_logger::Builder;
use serde::Serialize;
use sugars::{rc, refcell};

use dslab_core::component::Id;
use dslab_core::context::SimulationContext;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;
use dslab_core::simulation::Simulation;
use dslab_core::{cast, log_info};
use dslab_network::model::DataTransferCompleted;
use dslab_network::network::Network;
use dslab_network::topology_model::TopologyNetwork;

const TEST_NODES_AMOUNT: usize = 500;

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
            DataTransferCompleted { data: _ } => {
                log_info!(self.ctx, "Sender: data transfer completed");
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
                let new_size = 10000.0 - data.size;
                self.net
                    .borrow_mut()
                    .transfer_data(self.ctx.id(), data.src, new_size, data.src);
                log_info!(self.ctx, "Receiver: data transfer completed");
            }
        })
    }
}

fn main() {
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    let mut sim = Simulation::new(123);
    let now = Instant::now();

    let topology_rc = rc!(refcell!(Topology::new()));

    let topology_network_model = rc!(refcell!(TopologyNetwork::new(topology_rc.clone())));
    let topology_network = rc!(refcell!(Network::new_with_topology(
        topology_network_model.clone(),
        topology_rc.clone(),
        sim.create_context("net")
    )));
    sim.add_handler("net", topology_network.clone());

    let mut receivers = Vec::new();
    let mut senders = Vec::new();
    let mut compute_nodes = Vec::new();
    let mut commutator_nodes = Vec::new();

    for i in 1..TEST_NODES_AMOUNT + 1 {
        let receiver_name = format!("receiver_{}", i);
        let sender_name = format!("sender_{}", i);
        let compute_node_name = format!("compute_node_{}", i);
        let commutator_node_name = format!("commutator_{}", i);

        compute_nodes.push(compute_node_name.clone());
        commutator_nodes.push(commutator_node_name.clone());

        topology_network.borrow_mut().add_node(&compute_node_name, 100.0, 0.0);
        topology_network
            .borrow_mut()
            .add_node(&commutator_node_name, 100.0, 0.0);
        topology_network.borrow_mut().add_link(
            &compute_node_name,
            &commutator_node_name,
            i as f64,
            (i % 10 + 1) as f64 * 100.0,
        );

        let sender = DataTransferRequester::new(topology_network.clone(), sim.create_context(&sender_name));
        let sender_id = sim.add_handler(sender_name, rc!(refcell!(sender)));
        topology_network
            .borrow_mut()
            .set_location(sender_id, &compute_node_name);
        senders.push(sender_id);

        let receiver = DataReceiver::new(topology_network.clone(), sim.create_context(&receiver_name));
        let receiver_id = sim.add_handler(receiver_name, rc!(refcell!(receiver)));
        topology_network
            .borrow_mut()
            .set_location(receiver_id, &compute_node_name);
        receivers.push(receiver_id);
    }

    // Init commutator links
    for i in 1..TEST_NODES_AMOUNT + 1 {
        topology_network.borrow_mut().add_link(
            &commutator_nodes[i - 1],
            &commutator_nodes[i % TEST_NODES_AMOUNT],
            (i + 5) as f64,                    // Using 5 as a shift
            ((i + 5) % 10 + 1) as f64 * 100.0, // Using 5 as a shift
        );
    }

    println!("Topology create time: {} ms", now.elapsed().as_millis());

    topology_network.borrow_mut().init_topology();

    println!("Topology init time: {} ms", now.elapsed().as_millis());

    let mut client = sim.create_context("client");

    for i in 1..TEST_NODES_AMOUNT + 1 {
        client.emit(
            Start {
                size: 1000.0,
                receiver_id: receivers[(i + 40) % TEST_NODES_AMOUNT],
            },
            senders[i - 1],
            0.0,
        );
    }

    sim.step_until_no_events();
    println!("All operations process time: {} ms", now.elapsed().as_millis());
}
