use std::cell::RefCell;
use std::io::Write;
use std::rc::Rc;

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
use dslab_network::topology::Topology;
use dslab_network::topology_model::TopologyNetwork;

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
                let new_size = 1000.0 - data.size;
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

    let mut topology = Topology::new();
    // network nodes
    topology.add_node("sender_1", 100.0, 0.0);
    topology.add_node("sender_2", 100.0, 0.0);
    topology.add_node("switch_1", 100.0, 0.0);
    topology.add_node("switch_2", 100.0, 0.0);
    topology.add_node("receiver_1", 100.0, 0.0);
    topology.add_node("receiver_2", 100.0, 0.0);
    topology.add_node("sender_3", 100.0, 0.0);
    // network links
    topology.add_link("sender_1", "switch_1", 1.0, 100.0);
    topology.add_link("sender_2", "switch_1", 1.0, 90.0);
    topology.add_link("switch_1", "switch_2", 1.0, 50.0);
    topology.add_link("switch_2", "receiver_1", 1.0, 90.0);
    topology.add_link("switch_2", "receiver_2", 1.0, 10.0);
    // init topology
    topology.init();

    let topology_rc = rc!(refcell!(topology));
    let network_model = rc!(refcell!(TopologyNetwork::new(topology_rc.clone())));
    let network = Network::new_with_topology(network_model, topology_rc.clone(), sim.create_context("net"));

    let network_rc = rc!(refcell!(network));
    sim.add_handler("net", network_rc.clone());

    let sender_1 = DataSender::new(network_rc.clone(), sim.create_context("sender_1"));
    let sender_1_id = sim.add_handler("sender_1", rc!(refcell!(sender_1)));
    let sender_2 = DataSender::new(network_rc.clone(), sim.create_context("sender_2"));
    let sender_2_id = sim.add_handler("sender_2", rc!(refcell!(sender_2)));
    let sender_3 = DataSender::new(network_rc.clone(), sim.create_context("sender_3"));
    let sender_3_id = sim.add_handler("sender_3", rc!(refcell!(sender_3)));

    let receiver_1 = DataReceiver::new(network_rc.clone(), sim.create_context("receiver_1"));
    let receiver_1_id = sim.add_handler("receiver_1", rc!(refcell!(receiver_1)));
    let receiver_2 = DataReceiver::new(network_rc, sim.create_context("receiver_2"));
    let receiver_2_id = sim.add_handler("receiver_2", rc!(refcell!(receiver_2)));

    topology_rc.borrow_mut().set_location(sender_1_id, "sender_1");
    topology_rc.borrow_mut().set_location(sender_2_id, "sender_2");
    topology_rc.borrow_mut().set_location(sender_3_id, "sender_3");
    topology_rc.borrow_mut().set_location(receiver_1_id, "receiver_1");
    topology_rc.borrow_mut().set_location(receiver_2_id, "receiver_2");

    let mut client = sim.create_context("client");

    client.emit_now(
        Start {
            size: 100.0,
            receiver_id: receiver_1_id,
        },
        sender_1_id,
    );

    client.emit_now(
        Start {
            size: 100.0,
            receiver_id: receiver_2_id,
        },
        sender_2_id,
    );

    client.emit_now(
        Start {
            size: 100.0,
            receiver_id: receiver_1_id,
        },
        sender_3_id,
    );

    sim.step_until_no_events();
}
