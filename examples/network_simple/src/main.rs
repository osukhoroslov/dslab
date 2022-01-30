extern crate env_logger;
extern crate log;
use std::cell::RefCell;
use std::rc::Rc;

use core::actor::ActorId;
use core::sim::Simulation;
use network::constant_bandwidth_model::ConstantBandwidthNetwork;
use network::network_actor::{Network, NETWORK_ID};

fn main() {
    env_logger::init();

    let mut sim = Simulation::new(123);
    let client = ActorId::from("client");
    let sender = ActorId::from("sender");
    let reciever = ActorId::from("receiver");

    let constant_network_model = Rc::new(RefCell::new(ConstantBandwidthNetwork::new(10.0, 0.1)));
    let constant_network = Rc::new(RefCell::new(Network::new(constant_network_model)));
    sim.add_actor(NETWORK_ID, constant_network.clone());

    constant_network.borrow().transfer_data_from_sim(
        sender.clone(),
        reciever.clone(),
        100.0,
        sender.clone(),
        &mut sim,
    );
    constant_network.borrow().transfer_data_from_sim(
        sender.clone(),
        reciever.clone(),
        1000.0,
        sender.clone(),
        &mut sim,
    );
    constant_network.borrow().transfer_data_from_sim(
        sender.clone(),
        reciever.clone(),
        5.0,
        sender.clone(),
        &mut sim,
    );

    constant_network
        .borrow()
        .send_msg_from_sim("Hello World".to_string(), client.clone(), reciever.clone(), &mut sim);

    sim.step_until_no_events();
}
