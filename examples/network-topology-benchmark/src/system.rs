use std::cell::RefCell;
use std::rc::Rc;

use serde::Serialize;
use sugars::{rc, refcell};

use dslab_core::{cast, log_debug, Event, EventHandler, Id, Simulation, SimulationContext};
use dslab_network::{DataTransferCompleted, MessageDelivered, Network};

#[derive(Debug, Default)]
pub struct System {
    pub senders: Vec<u32>,
    pub receivers: Vec<u32>,
}

#[derive(Clone, Serialize)]
pub struct Start {
    pub data_size: f64,
    pub receiver_id: Id,
}

pub struct Sender {
    net: Rc<RefCell<Network>>,
    ctx: SimulationContext,
}

impl Sender {
    pub fn new(net: Rc<RefCell<Network>>, ctx: SimulationContext) -> Self {
        Self { net, ctx }
    }
}

impl EventHandler for Sender {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            Start { data_size, receiver_id } => {
                self.net
                    .borrow_mut()
                    .transfer_data(self.ctx.id(), receiver_id, data_size, receiver_id);
            }
            MessageDelivered { msg: _ } => {
                log_debug!(self.ctx, "Sender: data transfer completed");
            }
        })
    }
}

pub struct Receiver {
    net: Rc<RefCell<Network>>,
    ctx: SimulationContext,
}

impl Receiver {
    pub fn new(net: Rc<RefCell<Network>>, ctx: SimulationContext) -> Self {
        Self { net, ctx }
    }
}

impl EventHandler for Receiver {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            DataTransferCompleted { dt } => {
                self.net
                    .borrow_mut()
                    .send_msg("data transfer ack".to_string(), self.ctx.id(), dt.src);
                log_debug!(self.ctx, "Receiver: data transfer completed");
            }
        })
    }
}

pub fn build_system(sim: &mut Simulation, network_rc: Rc<RefCell<Network>>) -> System {
    let mut system = System::default();
    let mut network = network_rc.borrow_mut();
    let nodes = network.get_nodes();
    for node_name in nodes {
        if !node_name.starts_with("host_") {
            continue;
        }
        let sender_name = format!("sender_{}", &node_name[5..]);
        let receiver_name = format!("receiver_{}", &node_name[5..]);

        let sender = Sender::new(network_rc.clone(), sim.create_context(&sender_name));
        let sender_id = sim.add_handler(sender_name, rc!(refcell!(sender)));
        system.senders.push(sender_id);
        network.set_location(sender_id, &node_name);

        let receiver = Receiver::new(network_rc.clone(), sim.create_context(&receiver_name));
        let receiver_id = sim.add_handler(receiver_name, rc!(refcell!(receiver)));
        system.receivers.push(receiver_id);
        network.set_location(receiver_id, &node_name);
    }
    system
}
