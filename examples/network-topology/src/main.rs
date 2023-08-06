use std::cell::RefCell;
use std::io::Write;
use std::rc::Rc;

use env_logger::Builder;
use sugars::{boxed, rc, refcell};

use dslab_core::{cast, log_info, Event, EventHandler, Id, Simulation, SimulationContext};

use dslab_network::models::{SharedBandwidthNetworkModel, TopologyAwareNetworkModel};
use dslab_network::{DataTransferCompleted, Link, Network};

fn main() {
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    let mut sim = Simulation::new(123);

    // create network and define its topology by adding nodes and links
    let mut net = Network::new(boxed!(TopologyAwareNetworkModel::new()), sim.create_context("net"));
    net.add_node("client1", boxed!(SharedBandwidthNetworkModel::new(1000., 0.)));
    net.add_node("client2", boxed!(SharedBandwidthNetworkModel::new(1000., 0.)));
    net.add_node("client3", boxed!(SharedBandwidthNetworkModel::new(1000., 0.)));
    net.add_node("server1", boxed!(SharedBandwidthNetworkModel::new(1000., 0.)));
    net.add_node("server2", boxed!(SharedBandwidthNetworkModel::new(100., 0.)));
    net.add_node("switch1", boxed!(SharedBandwidthNetworkModel::new(1000., 0.)));
    net.add_node("switch2", boxed!(SharedBandwidthNetworkModel::new(1000., 0.)));
    net.add_full_duplex_link("client1", "switch1", Link::shared(100.0, 0.1));
    net.add_full_duplex_link("client2", "switch1", Link::shared(90.0, 0.1));
    net.add_full_duplex_link("client3", "switch1", Link::shared(50.0, 0.1));
    net.add_full_duplex_link("server1", "switch2", Link::shared(90.0, 0.1));
    net.add_full_duplex_link("server2", "switch2", Link::shared(20.0, 0.1));
    net.add_full_duplex_link("switch1", "switch2", Link::shared(50.0, 0.1));
    net.init_topology();

    // add network to simulations
    let net_rc = rc!(refcell!(net));
    sim.add_handler("net", net_rc.clone());

    // create client and server components, and add them to simulation
    let client1 = rc!(refcell!(Client::new(net_rc.clone(), sim.create_context("client1"))));
    let client1_id = sim.add_handler("client1", client1.clone());
    let client2 = rc!(refcell!(Client::new(net_rc.clone(), sim.create_context("client2"))));
    let client2_id = sim.add_handler("client2", client2.clone());
    let client3 = rc!(refcell!(Client::new(net_rc.clone(), sim.create_context("client3"))));
    let client3_id = sim.add_handler("client3", client3.clone());

    let server1 = Server::new(net_rc.clone(), sim.create_context("server1"));
    let server1_id = sim.add_handler("server1", rc!(refcell!(server1)));
    let server2 = Server::new(net_rc.clone(), sim.create_context("server2"));
    let server2_id = sim.add_handler("server2", rc!(refcell!(server2)));

    // bind client and server components to network nodes
    net_rc.borrow_mut().set_location(client1_id, "client1");
    net_rc.borrow_mut().set_location(client2_id, "client2");
    net_rc.borrow_mut().set_location(client3_id, "client3");
    net_rc.borrow_mut().set_location(server1_id, "server1");
    net_rc.borrow_mut().set_location(server2_id, "server2");

    // send data
    client1.borrow().send_data(100., server1_id);
    client2.borrow().send_data(200., server2_id);
    client3.borrow().send_data(400., server1_id);

    sim.step_until_no_events();
}

// Data Sender ---------------------------------------------------------------------------------------------------------

pub struct Client {
    net: Rc<RefCell<Network>>,
    ctx: SimulationContext,
}

impl Client {
    pub fn new(net: Rc<RefCell<Network>>, ctx: SimulationContext) -> Self {
        Self { net, ctx }
    }

    pub fn send_data(&self, size: f64, dst: Id) {
        let id = self.net.borrow_mut().transfer_data(self.ctx.id(), dst, size, dst);
        log_info!(self.ctx, "Started data transfer {} of size {} to {}", id, size, dst);
    }
}

impl EventHandler for Client {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            DataTransferCompleted { dt } => {
                log_info!(
                    self.ctx,
                    "Completed data transfer {} of size {} from {}",
                    dt.id,
                    dt.size,
                    dt.src
                );
            }
        })
    }
}

// Data Receiver -------------------------------------------------------------------------------------------------------

pub struct Server {
    net: Rc<RefCell<Network>>,
    ctx: SimulationContext,
}

impl Server {
    pub fn new(net: Rc<RefCell<Network>>, ctx: SimulationContext) -> Self {
        Self { net, ctx }
    }
}

impl EventHandler for Server {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            DataTransferCompleted { dt } => {
                log_info!(
                    self.ctx,
                    "Completed data transfer {} of size {} from {}",
                    dt.id,
                    dt.size,
                    dt.src
                );
                let size = 500.0 - dt.size;
                let id = self.net.borrow_mut().transfer_data(self.ctx.id(), dt.src, size, dt.src);
                log_info!(self.ctx, "Started data transfer {} of size {} to {}", id, size, dt.src);
            }
        })
    }
}
