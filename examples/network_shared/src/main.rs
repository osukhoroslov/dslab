extern crate env_logger;
extern crate log;

use std::cell::RefCell;
use std::rc::Rc;

use log::info;
use serde::Serialize;
use sugars::{rc, refcell};

use core::cast;
use core::context::SimulationContext;
use core::event::Event;
use core::handler::EventHandler;
use core::simulation::Simulation;
use network::model::DataTransferCompleted;
use network::network::Network;
use network::shared_bandwidth_model::SharedBandwidthNetwork;

#[derive(Serialize)]
pub struct Start {
    size: f64,
    receiver_id: String,
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
                    .transfer_data(self.ctx.id(), &receiver_id, size, &receiver_id);
            }
            DataTransferCompleted { data: _ } => {
                info!(
                    "System time: {}, Sender: {} recieved response",
                    self.ctx.time(),
                    self.ctx.id()
                );
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
                    .transfer_data(self.ctx.id(), &data.src, new_size, &data.src);
                info!("System time: {}, Receiver: {} Done", self.ctx.time(), self.ctx.id());
            }
        })
    }
}

fn main() {
    env_logger::init();

    let process_simple_send_1 = false;
    let process_check_order = false;
    let process_with_actors = false;
    let self_messages = true;

    let mut sim = Simulation::new(123);
    let sender = "sender";
    let receiver = "receiver";

    let shared_network_model = rc!(refcell!(SharedBandwidthNetwork::new(10.0, 0.1)));
    let shared_network = rc!(refcell!(Network::new(shared_network_model, sim.create_context("net"))));
    sim.add_handler("net", shared_network.clone());

    if process_simple_send_1 {
        info!("Simple send check 1");

        shared_network
            .borrow_mut()
            .transfer_data(sender, receiver, 100.0, sender);
        shared_network
            .borrow_mut()
            .transfer_data(sender, receiver, 1000.0, sender);
        shared_network.borrow_mut().transfer_data(sender, receiver, 5.0, sender);

        shared_network
            .borrow_mut()
            .send_msg("Hello World".to_string(), sender, receiver);

        sim.step_until_no_events();
    }

    if process_check_order {
        info!("Data order check");

        for _i in 1..10 {
            shared_network
                .borrow_mut()
                .transfer_data(sender, receiver, 1000.0, sender);
        }
        shared_network
            .borrow_mut()
            .send_msg("Hello World".to_string(), sender, receiver);

        sim.step_until_no_events();
    }

    if process_with_actors {
        info!("With actors check");
        let mut receivers = Vec::new();
        let mut senders = Vec::new();

        for i in 1..10 {
            let receiver_id = "receiver_".to_string() + &i.to_string();
            let receiver = DataReceiver::new(shared_network.clone(), sim.create_context(&receiver_id));
            sim.add_handler(&receiver_id, rc!(refcell!(receiver)));
            receivers.push(receiver_id);

            let sender_id = "sender_".to_string() + &i.to_string();
            let sender = DataTransferRequester::new(shared_network.clone(), sim.create_context(&sender_id));
            sim.add_handler(&sender_id, rc!(refcell!(sender)));
            senders.push(sender_id);
        }

        let mut client = sim.create_context("app");
        for i in 1..10 {
            client.emit(
                Start {
                    size: (i as f64) * 100.0,
                    receiver_id: receivers[i - 1].clone(),
                },
                &senders[i - 1],
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
            let receiver = DataReceiver::new(shared_network.clone(), sim.create_context(&receiver_id));
            sim.add_handler(&receiver_id, rc!(refcell!(receiver)));
            let receiver_host_id = "host_".to_string() + &receiver_id;
            shared_network.borrow_mut().add_host(&receiver_host_id, 1000.0, 0.0);
            shared_network
                .borrow_mut()
                .set_location(&receiver_id, &receiver_host_id);
            distant_receivers.push(receiver_id);

            let local_receiver_id = "local_receiver_".to_string() + &i.to_string();
            let local_receiver = DataReceiver::new(shared_network.clone(), sim.create_context(&local_receiver_id));
            sim.add_handler(&local_receiver_id, rc!(refcell!(local_receiver)));
            shared_network
                .borrow_mut()
                .set_location(&local_receiver_id, "localhost");
            local_receivers.push(local_receiver_id);
        }

        let sender_id = "sender";
        let sender = DataTransferRequester::new(shared_network.clone(), sim.create_context(sender_id));
        sim.add_handler(sender_id, rc!(refcell!(sender)));
        shared_network.borrow_mut().set_location(sender_id, "localhost");

        let mut client = sim.create_context("app");
        for i in 1..10 {
            client.emit(
                Start {
                    size: 100.0,
                    receiver_id: distant_receivers[i - 1].clone(),
                },
                sender_id,
                0.0,
            );
            client.emit(
                Start {
                    size: 100.0,
                    receiver_id: local_receivers[i - 1].clone(),
                },
                sender_id,
                0.0,
            );
        }

        sim.step_until_no_events();
    }
}
