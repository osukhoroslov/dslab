extern crate env_logger;
extern crate log;
use log::info;
use std::cell::RefCell;
use std::rc::Rc;

use core::actor::{Actor, ActorContext, ActorId, Event};
use core::match_event;
use core::sim::Simulation;
use network::model::DataDelivery;
use network::network_actor::{NetworkActor, NETWORK_ID};
use network::shared_throughput_model::SharedThroughputNetwork;

// Counter for network ids
// ACTORS //////////////////////////////////////////////////////////////////////////////////////////
#[derive(Debug)]
pub struct Start {
    size: f64,
    receiver_id: ActorId,
}

pub struct DataTransferRequester {
    net: Rc<RefCell<NetworkActor>>,
}

impl DataTransferRequester {
    pub fn new(net: Rc<RefCell<NetworkActor>>) -> Self {
        Self { net }
    }
}

impl Actor for DataTransferRequester {
    fn on(&mut self, event: Box<dyn Event>, _from: ActorId, ctx: &mut ActorContext) {
        match_event!( event {
            Start { size, receiver_id } => {
                self.net.borrow_mut().transfer_data(ctx.id.clone(), receiver_id.clone(), *size, ctx);
            },
            DataDelivery { data: _ } => {
                info!("System time: {}, Sender: {} Done", ctx.time(), ctx.id.clone());
            },
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}

pub struct DataReceiver {
    net: Rc<RefCell<NetworkActor>>,
}

impl DataReceiver {
    pub fn new(net: Rc<RefCell<NetworkActor>>) -> Self {
        Self { net }
    }
}

impl Actor for DataReceiver {
    fn on(&mut self, event: Box<dyn Event>, _from: ActorId, ctx: &mut ActorContext) {
        match_event!( event {
            DataDelivery { data } => {
                let new_size = 1000.0 - data.size;
                self.net.borrow_mut().transfer_data(ctx.id.clone(), data.source.clone(), new_size, ctx);
                info!("System time: {}, Receiver: {} Done", ctx.time(), ctx.id.clone());
            },
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}

fn main() {
    env_logger::init();

    let process_simple_send_1 = true;
    let process_check_order = true;
    let process_with_actors = true;

    let mut sim = Simulation::new(123);
    let sender_actor = ActorId::from("sender");
    let receiver_actor = ActorId::from("receiver");

    let shared_network_model = Rc::new(RefCell::new(SharedThroughputNetwork::new(10.0)));
    let shared_network = Rc::new(RefCell::new(NetworkActor::new(shared_network_model, 0.1)));
    sim.add_actor(NETWORK_ID, shared_network.clone());

    if process_simple_send_1 {
        info!("Simple send check 1");

        shared_network.borrow_mut().transfer_data_from_sim(
            sender_actor.clone(),
            receiver_actor.clone(),
            100.0,
            &mut sim,
        );
        shared_network.borrow_mut().transfer_data_from_sim(
            sender_actor.clone(),
            receiver_actor.clone(),
            1000.0,
            &mut sim,
        );
        shared_network
            .borrow_mut()
            .transfer_data_from_sim(sender_actor.clone(), receiver_actor.clone(), 5.0, &mut sim);

        shared_network
            .borrow_mut()
            .send_message_from_sim("Hello World".to_string(), receiver_actor.clone(), &mut sim);

        sim.step_until_no_events();
    }

    if process_check_order {
        info!("Data order check");

        for _i in 1..10 {
            shared_network.borrow_mut().transfer_data_from_sim(
                sender_actor.clone(),
                receiver_actor.clone(),
                1000.0,
                &mut sim,
            );
        }
        shared_network
            .borrow_mut()
            .send_message_from_sim("Hello World".to_string(), receiver_actor.clone(), &mut sim);

        sim.step_until_no_events();
    }

    if process_with_actors {
        info!("With actors check");
        let mut receivers = Vec::new();
        let mut senders = Vec::new();

        for i in 1..10 {
            let receiver_id = "receiver_".to_string() + &i.to_string();
            let receiver = Rc::new(RefCell::new(DataReceiver::new(shared_network.clone())));
            let receiver_actor = sim.add_actor(&receiver_id, receiver);
            receivers.push(receiver_actor);

            let sender_id = "sender_".to_string() + &i.to_string();
            let sender = Rc::new(RefCell::new(DataTransferRequester::new(shared_network.clone())));
            let sender_actor = sim.add_actor(&sender_id, sender);
            senders.push(sender_actor);
        }

        let initial_actor = ActorId::from("app");
        for i in 1..10 {
            sim.add_event(
                Start {
                    size: (i as f64) * 100.0,
                    receiver_id: receivers[i - 1].clone(),
                },
                initial_actor.clone(),
                senders[i - 1].clone(),
                0.0,
            );
        }

        sim.step_until_no_events();
    }
}
