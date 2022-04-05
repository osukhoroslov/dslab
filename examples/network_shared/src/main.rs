use std::cell::RefCell;
use std::io::Write;
use std::rc::Rc;

use env_logger::Builder;
use log::info;
use serde::Serialize;
use sugars::{rc, refcell};

use network::model::DataTransferCompleted;
use network::network::Network;
use network::shared_bandwidth_model::SharedBandwidthNetwork;
use simcore::component::Id;
use simcore::context::SimulationContext;
use simcore::event::Event;
use simcore::handler::EventHandler;
use simcore::simulation::Simulation;
use simcore::{cast, log_info};

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

    let process_simple_send_1 = false;
    let process_check_order = false;
    let process_with_actors = false;
    let self_messages = true;

    let mut sim = Simulation::new(123);
    let sender_id = 1;
    let receiver_id = 2;

    let shared_network_model = rc!(refcell!(SharedBandwidthNetwork::new(10.0, 0.1)));
    let shared_network = rc!(refcell!(Network::new(shared_network_model, sim.create_context("net"))));
    sim.add_handler("net", shared_network.clone());

    if process_simple_send_1 {
        info!("Simple send check 1");

        shared_network
            .borrow_mut()
            .transfer_data(sender_id, receiver_id, 100.0, sender_id);
        shared_network
            .borrow_mut()
            .transfer_data(sender_id, receiver_id, 1000.0, sender_id);
        shared_network
            .borrow_mut()
            .transfer_data(sender_id, receiver_id, 5.0, sender_id);

        shared_network
            .borrow_mut()
            .send_msg("Hello World".to_string(), sender_id, receiver_id);

        sim.step_until_no_events();
    }

    if process_check_order {
        info!("Data order check");

        for _i in 1..10 {
            shared_network
                .borrow_mut()
                .transfer_data(sender_id, receiver_id, 1000.0, sender_id);
        }
        shared_network
            .borrow_mut()
            .send_msg("Hello World".to_string(), sender_id, receiver_id);

        sim.step_until_no_events();
    }

    if process_with_actors {
        info!("With actors check");
        let mut receivers = Vec::new();
        let mut senders = Vec::new();

        for i in 1..10 {
            let receiver_name = format!("receiver_{}", i);
            let receiver = DataReceiver::new(shared_network.clone(), sim.create_context(&receiver_name));
            let receiver_id = sim.add_handler(&receiver_name, rc!(refcell!(receiver)));
            receivers.push(receiver_id);

            let sender_name = format!("sender_{}", i);
            let sender = DataTransferRequester::new(shared_network.clone(), sim.create_context(&sender_name));
            let sender_id = sim.add_handler(&sender_name, rc!(refcell!(sender)));
            senders.push(sender_id);
        }

        let mut client = sim.create_context("client");
        for i in 1..10 {
            client.emit(
                Start {
                    size: (i as f64) * 100.0,
                    receiver_id: receivers[i - 1],
                },
                senders[i - 1],
                0.0,
            );
        }

        sim.step_until_no_events();
    }

    if self_messages {
        info!("Self Messages");
        let mut distant_receivers = Vec::new();
        let mut local_receivers = Vec::new();

        shared_network.borrow_mut().add_node("localhost", 1000.0, 0.0);

        for i in 1..10 {
            let receiver_name = format!("receiver_{}", i);
            let receiver = DataReceiver::new(shared_network.clone(), sim.create_context(&receiver_name));
            let receiver_id = sim.add_handler(&receiver_name, rc!(refcell!(receiver)));
            let receiver_host = format!("host_{}", &receiver_name);
            shared_network.borrow_mut().add_node(&receiver_host, 1000.0, 0.0);
            shared_network.borrow_mut().set_location(receiver_id, &receiver_host);
            distant_receivers.push(receiver_id);

            let local_receiver_name = format!("local_receiver_{}", i);
            let local_receiver = DataReceiver::new(shared_network.clone(), sim.create_context(&local_receiver_name));
            let local_receiver_id = sim.add_handler(&local_receiver_name, rc!(refcell!(local_receiver)));
            shared_network.borrow_mut().set_location(local_receiver_id, "localhost");
            local_receivers.push(local_receiver_id);
        }

        let sender_name = "sender";
        let sender = DataTransferRequester::new(shared_network.clone(), sim.create_context(sender_name));
        let sender_id = sim.add_handler(sender_name, rc!(refcell!(sender)));
        shared_network.borrow_mut().set_location(sender_id, "localhost");

        let mut client = sim.create_context("client");
        for i in 1..10 {
            client.emit(
                Start {
                    size: 100.0,
                    receiver_id: distant_receivers[i - 1],
                },
                sender_id,
                0.0,
            );
            client.emit(
                Start {
                    size: 100.0,
                    receiver_id: local_receivers[i - 1],
                },
                sender_id,
                0.0,
            );
        }

        sim.step_until_no_events();
    }
}
