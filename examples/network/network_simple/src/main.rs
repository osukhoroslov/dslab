extern crate env_logger;
extern crate log;
use std::cell::RefCell;
use std::rc::Rc;

use core::actor::ActorId;
use core::sim::Simulation;
use network::constant_throughput_model::ConstantThroughputNetwork;
use network::network_actor::{NetworkActor, NETWORK_ID};

fn main() {
    env_logger::init();

    let mut sim = Simulation::new(123);
    let sender_actor = ActorId::from("sender");
    let receiver_actor = ActorId::from("receiver");

    let constant_network_model = Rc::new(RefCell::new(ConstantThroughputNetwork::new(10.0)));
    let constant_network = Rc::new(RefCell::new(NetworkActor::new(constant_network_model, 0.1)));
    sim.add_actor(NETWORK_ID, constant_network.clone());

    constant_network
        .borrow_mut()
        .transfer_data_from_sim(sender_actor.clone(), receiver_actor.clone(), 100.0, &mut sim);
    constant_network.borrow_mut().transfer_data_from_sim(
        sender_actor.clone(),
        receiver_actor.clone(),
        1000.0,
        &mut sim,
    );
    constant_network
        .borrow_mut()
        .transfer_data_from_sim(sender_actor.clone(), receiver_actor.clone(), 5.0, &mut sim);

    constant_network
        .borrow_mut()
        .send_message_from_sim("Hello World".to_string(), receiver_actor.clone(), &mut sim);

    sim.step_until_no_events();
}
