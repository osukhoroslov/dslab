extern crate env_logger;
extern crate log;

use sugars::{rc, refcell};

use core::simulation::Simulation;
use network::constant_bandwidth_model::ConstantBandwidthNetwork;
use network::network::Network;

fn main() {
    env_logger::init();

    let mut sim = Simulation::new(123);
    let sender = "sender";
    let receiver = "receiver";

    let constant_network_model = rc!(refcell!(ConstantBandwidthNetwork::new(10.0, 0.1)));
    let constant_network = rc!(refcell!(Network::new(
        constant_network_model,
        sim.create_context("net")
    )));
    sim.add_handler("net", constant_network.clone());

    constant_network
        .borrow_mut()
        .transfer_data(sender, receiver, 100.0, sender);
    constant_network
        .borrow_mut()
        .transfer_data(sender, receiver, 1000.0, sender);
    constant_network
        .borrow_mut()
        .transfer_data(sender, receiver, 5.0, sender);

    constant_network
        .borrow_mut()
        .send_msg("Hello World".to_string(), sender, receiver);

    sim.step_until_no_events();
}
