extern crate env_logger;
extern crate log;
use log::info;
use std::cell::RefCell;
use std::rc::Rc;

use core::actor::{Actor, ActorContext, ActorId, Event};
use core::cast;
use core::sim::Simulation;
use network::model::DataTransferCompleted;
use network::network_actor::{Network, NETWORK_ID};
use network::shared_bandwidth_model::SharedBandwidthNetwork;

#[derive(Debug)]
pub struct Start {
    size: f64,
    receiver_id: ActorId,
}

pub struct DataTransferRequester {
    net: Rc<RefCell<Network>>,
}

impl DataTransferRequester {
    pub fn new(net: Rc<RefCell<Network>>) -> Self {
        Self { net }
    }
}

impl Actor for DataTransferRequester {
    fn on(&mut self, event: Box<dyn Event>, _from: ActorId, ctx: &mut ActorContext) {
        cast!(match event {
            Start { size, receiver_id } => {
                self.net
                    .borrow()
                    .transfer_data(ctx.id.clone(), receiver_id.clone(), *size, receiver_id.clone(), ctx);
            }
            DataTransferCompleted { data: _ } => {
                info!(
                    "System time: {}, Sender: {} recieved response",
                    ctx.time(),
                    ctx.id.clone()
                );
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}

pub struct DataReceiver {
    net: Rc<RefCell<Network>>,
}

impl DataReceiver {
    pub fn new(net: Rc<RefCell<Network>>) -> Self {
        Self { net }
    }
}

impl Actor for DataReceiver {
    fn on(&mut self, event: Box<dyn Event>, _from: ActorId, ctx: &mut ActorContext) {
        cast!(match event {
            DataTransferCompleted { data } => {
                let new_size = 1000.0 - data.size;
                self.net
                    .borrow()
                    .transfer_data(ctx.id.clone(), data.src.clone(), new_size, data.src.clone(), ctx);
                info!("System time: {}, Receiver: {} Done", ctx.time(), ctx.id.clone());
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}

fn main() {
    env_logger::init();

    let process_simple_send_1 = false;
    let process_check_order = false;
    let process_with_actors = false;
    let self_messages = true;

    let mut sim = Simulation::new(123);
    let client = ActorId::from("client");
    let sender = ActorId::from("sender");
    let receiver = ActorId::from("receiver");

    let shared_network_model = Rc::new(RefCell::new(SharedBandwidthNetwork::new(10.0, 0.1)));
    let shared_network = Rc::new(RefCell::new(Network::new(shared_network_model)));
    sim.add_actor(NETWORK_ID, shared_network.clone());

    if process_simple_send_1 {
        info!("Simple send check 1");

        shared_network.borrow().transfer_data_from_sim(
            sender.clone(),
            receiver.clone(),
            100.0,
            sender.clone(),
            &mut sim,
        );
        shared_network.borrow().transfer_data_from_sim(
            sender.clone(),
            receiver.clone(),
            1000.0,
            sender.clone(),
            &mut sim,
        );
        shared_network
            .borrow()
            .transfer_data_from_sim(sender.clone(), receiver.clone(), 5.0, sender.clone(), &mut sim);

        shared_network.borrow().send_msg_from_sim(
            "Hello World".to_string(),
            client.clone(),
            receiver.clone(),
            &mut sim,
        );

        sim.step_until_no_events();
    }

    if process_check_order {
        info!("Data order check");

        for _i in 1..10 {
            shared_network.borrow().transfer_data_from_sim(
                sender.clone(),
                receiver.clone(),
                1000.0,
                sender.clone(),
                &mut sim,
            );
        }
        shared_network.borrow().send_msg_from_sim(
            "Hello World".to_string(),
            client.clone(),
            receiver.clone(),
            &mut sim,
        );

        sim.step_until_no_events();
    }

    if process_with_actors {
        info!("With actors check");
        let mut receivers = Vec::new();
        let mut senders = Vec::new();

        for i in 1..10 {
            let receiver_id = "receiver_".to_string() + &i.to_string();
            let receiver = Rc::new(RefCell::new(DataReceiver::new(shared_network.clone())));
            let receiver = sim.add_actor(&receiver_id, receiver);
            receivers.push(receiver);

            let sender_id = "sender_".to_string() + &i.to_string();
            let sender = Rc::new(RefCell::new(DataTransferRequester::new(shared_network.clone())));
            let sender = sim.add_actor(&sender_id, sender);
            senders.push(sender);
        }

        let client = ActorId::from("app");
        for i in 1..10 {
            sim.add_event(
                Start {
                    size: (i as f64) * 100.0,
                    receiver_id: receivers[i - 1].clone(),
                },
                client.clone(),
                senders[i - 1].clone(),
                0.0,
            );
        }

        sim.step_until_no_events();
    }

    if self_messages {
        info!("Self Messages");
        let mut distant_receivers = Vec::new();
        let mut local_receivers = Vec::new();

        shared_network.borrow_mut().add_host("localhost", 1000.0, 0.0);

        for i in 1..10 {
            let receiver_id = "receiver_".to_string() + &i.to_string();
            let receiver = Rc::new(RefCell::new(DataReceiver::new(shared_network.clone())));
            let receiver = sim.add_actor(&receiver_id, receiver);
            let receiver_host_id = "host_".to_string() + &receiver_id;
            shared_network.borrow_mut().add_host(&receiver_host_id, 1000.0, 0.0);
            shared_network
                .borrow_mut()
                .set_actor_host(receiver.clone(), &receiver_host_id);
            distant_receivers.push(receiver);

            let local_receiver_id = "local_receiver_".to_string() + &i.to_string();
            let local_receiver = Rc::new(RefCell::new(DataReceiver::new(shared_network.clone())));
            let local_receiver = sim.add_actor(&local_receiver_id, local_receiver);
            shared_network
                .borrow_mut()
                .set_actor_host(local_receiver.clone(), "localhost");
            local_receivers.push(local_receiver);
        }

        let sender_id = "sender".to_string();
        let sender = Rc::new(RefCell::new(DataTransferRequester::new(shared_network.clone())));
        let sender = sim.add_actor(&sender_id, sender);
        shared_network.borrow_mut().set_actor_host(sender.clone(), "localhost");

        let client = ActorId::from("app");
        for i in 1..10 {
            sim.add_event(
                Start {
                    size: 100.0,
                    receiver_id: distant_receivers[i - 1].clone(),
                },
                client.clone(),
                sender.clone(),
                0.0,
            );
            sim.add_event(
                Start {
                    size: 100.0,
                    receiver_id: local_receivers[i - 1].clone(),
                },
                client.clone(),
                sender.clone(),
                0.0,
            );
        }

        sim.step_until_no_events();
    }
}
