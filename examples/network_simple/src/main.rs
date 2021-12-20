extern crate log;
extern crate env_logger;
use log::info;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};

use core::actor::{Actor, ActorContext, ActorId, Event};
use core::match_event;
use core::sim::Simulation;
use network::network_actor::NetworkActor;
use network::model::{Data, Message, DataTransferRequest, MessageSend, DataDelivery};
use network::constant_throughput_model::ConstantThroughputNetwork;
use network::shared_throughput_model::SharedThroughputNetwork;

// Counter for network ids
static COUNTER: AtomicUsize = AtomicUsize::new(1);

// ACTORS //////////////////////////////////////////////////////////////////////////////////////////
#[derive(Debug)]
pub struct Start {
    size: f64,
    receiver_id: ActorId,
}

pub struct DataTransferRequester {
    network_id: ActorId,
}

impl DataTransferRequester {
    pub fn new(network_id: ActorId) -> Self {
        Self { network_id }
    }
}

impl Actor for DataTransferRequester {
    fn on(&mut self, event: Box<dyn Event>, _from: ActorId, ctx: &mut ActorContext) {
        match_event!( event {
            Start { size, receiver_id } => {
                let data_id = COUNTER.fetch_add(1, Ordering::Relaxed);
                let data = Data{ id: data_id, source: ctx.id.clone(), dest: receiver_id.clone(), size: *size};
                ctx.emit(DataTransferRequest { data }, self.network_id.clone(), 0.0);
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
    network_id: ActorId,
}

impl DataReceiver {
    pub fn new(network_id: ActorId) -> Self {
        Self { network_id }
    }
}

impl Actor for DataReceiver {
    fn on(&mut self, event: Box<dyn Event>, _from: ActorId, ctx: &mut ActorContext) {
        match_event!( event {
            DataDelivery { data } => {
                let data_id = COUNTER.fetch_add(1, Ordering::Relaxed);
                let new_size = 1000.0 - data.size;
                let data = Data{ id: data_id, source: ctx.id.clone(), dest: data.source.clone(), size: new_size};
                ctx.emit(DataTransferRequest { data }, self.network_id.clone(), 0.0);
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

    let process_simple_send_1 = false;
    let process_simple_send_2 = false;
    let process_check_order = true;
    let process_with_actors = false;

    let mut sim = Simulation::new(123);
    let sender_actor = ActorId::from("sender");
    let receiver_actor = ActorId::from("receiver");

    let shared_network_model = Rc::new(RefCell::new(SharedThroughputNetwork::new(10.0, 0.1)));
    let shared_network = Rc::new(RefCell::new(NetworkActor::new(
        shared_network_model
    )));
    let shared_network_actor = sim.add_actor("shared_network", shared_network);

    let constant_network_model = Rc::new(RefCell::new(ConstantThroughputNetwork::new(10.0, 0.1)));
    let constant_network = Rc::new(RefCell::new(NetworkActor::new(
        constant_network_model
    )));
    let constant_network_actor = sim.add_actor("constant_network", constant_network);

    if process_simple_send_1 {
        info!("Simple send check 1");
        let msg = Message {
            id: COUNTER.fetch_add(1, Ordering::Relaxed),
            source: sender_actor.clone(),
            dest: receiver_actor.clone(),
            data: "Hello World".to_string(),
        };

        let data1 = Data {
            id: COUNTER.fetch_add(1, Ordering::Relaxed),
            source: sender_actor.clone(),
            dest: receiver_actor.clone(),
            size: 100.0,
        };
        sim.add_event(
            DataTransferRequest { data: data1 },
            sender_actor.clone(),
            shared_network_actor.clone(),
            0.,
        );

        let data2 = Data {
            id: COUNTER.fetch_add(1, Ordering::Relaxed),
            source: sender_actor.clone(),
            dest: receiver_actor.clone(),
            size: 1000.0,
        };
        sim.add_event(
            DataTransferRequest { data: data2 },
            sender_actor.clone(),
            shared_network_actor.clone(),
            0.,
        );

        let data3 = Data {
            id: COUNTER.fetch_add(1, Ordering::Relaxed),
            source: sender_actor.clone(),
            dest: receiver_actor.clone(),
            size: 5.0,
        };
        sim.add_event(
            DataTransferRequest { data: data3 },
            sender_actor.clone(),
            shared_network_actor.clone(),
            0.,
        );

        sim.add_event(
            MessageSend { message: msg },
            sender_actor.clone(),
            shared_network_actor.clone(),
            0.,
        );

        sim.step_until_no_events();
    }

    if process_simple_send_2 {
        info!("Simple send check 2");
        let msg = Message {
            id: COUNTER.fetch_add(1, Ordering::Relaxed),
            source: sender_actor.clone(),
            dest: receiver_actor.clone(),
            data: "Hello World".to_string(),
        };

        let data1 = Data {
            id: COUNTER.fetch_add(1, Ordering::Relaxed),
            source: sender_actor.clone(),
            dest: receiver_actor.clone(),
            size: 100.0,
        };
        sim.add_event(
            DataTransferRequest { data: data1 },
            sender_actor.clone(),
            constant_network_actor.clone(),
            0.,
        );

        let data2 = Data {
            id: COUNTER.fetch_add(1, Ordering::Relaxed),
            source: sender_actor.clone(),
            dest: receiver_actor.clone(),
            size: 1000.0,
        };
        sim.add_event(
            DataTransferRequest { data: data2 },
            sender_actor.clone(),
            constant_network_actor.clone(),
            0.,
        );

        let data3 = Data {
            id: COUNTER.fetch_add(1, Ordering::Relaxed),
            source: sender_actor.clone(),
            dest: receiver_actor.clone(),
            size: 5.0,
        };
        sim.add_event(
            DataTransferRequest { data: data3 },
            sender_actor.clone(),
            constant_network_actor.clone(),
            0.,
        );

        sim.add_event(
            MessageSend { message: msg },
            sender_actor.clone(),
            constant_network_actor.clone(),
            0.,
        );

        sim.step_until_no_events();
    }

    if process_check_order {
        info!("Data order check");
        let msg = Message {
            id: COUNTER.fetch_add(1, Ordering::Relaxed),
            source: sender_actor.clone(),
            dest: receiver_actor.clone(),
            data: "Hello World".to_string(),
        };

        for _i in 1..10 {
            let data1 = Data {
                id: COUNTER.fetch_add(1, Ordering::Relaxed),
                source: sender_actor.clone(),
                dest: receiver_actor.clone(),
                size: 1000.0,
            };
            sim.add_event(
                DataTransferRequest { data: data1 },
                sender_actor.clone(),
                shared_network_actor.clone(),
                0.,
            );
        }

        sim.add_event(
            MessageSend { message: msg },
            sender_actor.clone(),
            shared_network_actor.clone(),
            0.,
        );

        sim.step_until_no_events();
    }

    if process_with_actors {
        info!("With actors check");
        let mut receivers = Vec::new();
        let mut senders = Vec::new();

        for i in 1..10 {
            let receiver_id = "receiver_".to_string() + &i.to_string();
            let receiver = Rc::new(RefCell::new(DataReceiver::new(
                shared_network_actor.clone(),
            )));
            let receiver_actor = sim.add_actor(&receiver_id, receiver);
            receivers.push(receiver_actor);

            let sender_id = "sender_".to_string() + &i.to_string();
            let sender = Rc::new(RefCell::new(DataTransferRequester::new(shared_network_actor.clone())));
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
