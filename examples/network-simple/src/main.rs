use std::io::Write;

use env_logger::Builder;
use sugars::{rc, refcell};

use dslab_core::simulation::Simulation;
use dslab_network::constant_bandwidth_model::ConstantBandwidthNetwork;
use dslab_network::network::Network;

fn main() {
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    let mut sim = Simulation::new(123);
    let sender_id = 1;
    let receiver_id = 2;

    let constant_network_model = rc!(refcell!(ConstantBandwidthNetwork::new(10.0, 0.1)));
    let constant_network = rc!(refcell!(Network::new(
        constant_network_model,
        sim.create_context("net")
    )));
    sim.add_handler("net", constant_network.clone());

    constant_network
        .borrow_mut()
        .transfer_data(sender_id, receiver_id, 100.0, sender_id);
    constant_network
        .borrow_mut()
        .transfer_data(sender_id, receiver_id, 1000.0, sender_id);
    constant_network
        .borrow_mut()
        .transfer_data(sender_id, receiver_id, 5.0, sender_id);

    constant_network
        .borrow_mut()
        .send_msg("Hello World".to_string(), sender_id, receiver_id);

    sim.step_until_no_events();
}
