use std::cell::RefCell;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};

use dslab_core::component::Id;
use dslab_core::context::SimulationContext;
use dslab_core::event::{Event, EventData};
use dslab_core::handler::EventHandler;
use dslab_core::{cast, log_debug};

use crate::model::*;
use crate::topology::Topology;

// pub const NETWORK_ID: &str = "net";

pub struct Network {
    network_model: Rc<RefCell<dyn NetworkModel>>,
    topology: Rc<RefCell<Topology>>,
    id_counter: AtomicUsize,
    ctx: SimulationContext,
}

impl Network {
    pub fn new(network_model: Rc<RefCell<dyn NetworkModel>>, ctx: SimulationContext) -> Self {
        Self {
            network_model,
            topology: Rc::new(RefCell::new(Topology::new())),
            id_counter: AtomicUsize::new(1),
            ctx,
        }
    }

    pub fn new_with_topology(
        network_model: Rc<RefCell<dyn NetworkModel>>,
        topology: Rc<RefCell<Topology>>,
        ctx: SimulationContext,
    ) -> Self {
        Self {
            network_model,
            topology,
            id_counter: AtomicUsize::new(1),
            ctx,
        }
    }

    pub fn add_node(&mut self, node_id: &str, local_bandwidth: f64, local_latency: f64) {
        self.topology
            .borrow_mut()
            .add_node(node_id, local_bandwidth, local_latency)
    }

    pub fn add_link(&mut self, node1: &str, node2: &str, latency: f64, bandwidth: f64) {
        self.topology.borrow_mut().add_link(node1, node2, latency, bandwidth);
        self.network_model.borrow_mut().recalculate_operations(&mut self.ctx);
    }

    pub fn init_topology(&mut self) {
        self.topology.borrow_mut().init();
    }

    pub fn set_location(&mut self, id: Id, node_name: &str) {
        self.topology.borrow_mut().set_location(id, node_name)
    }

    pub fn check_same_node(&self, id1: Id, id2: Id) -> bool {
        self.topology.borrow().check_same_node(id1, id2)
    }

    pub fn get_nodes(&self) -> Vec<String> {
        self.topology.borrow().get_nodes()
    }

    pub fn send_msg(&mut self, message: String, src: Id, dest: Id) -> usize {
        let msg_id = self.id_counter.fetch_add(1, Ordering::Relaxed);
        let msg = Message {
            id: msg_id,
            src,
            dest,
            data: message,
        };
        self.ctx.emit_self_now(MessageSend { message: msg });
        msg_id
    }

    pub fn send_event<T: EventData>(&mut self, data: T, src: Id, dest: Id) {
        log_debug!(self.ctx, "{} sent event to {}", src, dest);

        let latency = if self.check_same_node(src, dest) {
            self.topology.borrow().get_local_latency(src, dest)
        } else {
            self.network_model.borrow().latency(src, dest)
        };
        self.ctx.emit_as(data, src, dest, latency);
    }

    pub fn transfer_data(&mut self, src: Id, dest: Id, size: f64, notification_dest: Id) -> usize {
        let data_id = self.id_counter.fetch_add(1, Ordering::Relaxed);
        let data = Data {
            id: data_id,
            src,
            dest,
            size,
            notification_dest,
        };
        self.ctx.emit_self_now(DataTransferRequest { data });
        data_id
    }

    pub fn bandwidth(&self, src: Id, dest: Id) -> f64 {
        self.network_model.borrow().bandwidth(src, dest)
    }
}

impl EventHandler for Network {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            MessageSend { message } => {
                log_debug!(
                    self.ctx,
                    "{} sent message '{}' to {}",
                    message.src,
                    message.data,
                    message.dest.clone()
                );

                let latency = if self.check_same_node(message.src, message.dest) {
                    self.topology.borrow().get_local_latency(message.src, message.dest)
                } else {
                    self.network_model.borrow().latency(message.src, message.dest)
                };
                let message_recieve_event = MessageReceive { message };
                self.ctx.emit_self(message_recieve_event, latency);
            }
            MessageReceive { message } => {
                log_debug!(
                    self.ctx,
                    "{} received message '{}' from {}",
                    message.dest,
                    message.data,
                    message.src
                );
                self.ctx.emit_now(
                    MessageDelivery {
                        message: message.clone(),
                    },
                    message.dest,
                );
            }
            DataTransferRequest { data } => {
                log_debug!(
                    self.ctx,
                    "new data transfer {} from {} to {} of size {}",
                    data.id,
                    data.src,
                    data.dest,
                    data.size
                );
                let latency = if self.check_same_node(data.src, data.dest) {
                    self.topology.borrow().get_local_latency(data.src, data.dest)
                } else {
                    self.network_model.borrow().latency(data.src, data.dest)
                };
                self.ctx.emit_self(StartDataTransfer { data }, latency);
            }
            StartDataTransfer { data } => {
                if !self.check_same_node(data.src, data.dest) {
                    self.network_model.borrow_mut().send_data(data, &mut self.ctx);
                } else {
                    self.topology.borrow_mut().local_send_data(data, &mut self.ctx)
                }
            }
            DataReceive { data } => {
                log_debug!(
                    self.ctx,
                    "completed data transfer {} from {} to {} of size {}",
                    data.id,
                    data.src,
                    data.dest,
                    data.size
                );
                if !self.check_same_node(data.src, data.dest) {
                    self.network_model
                        .borrow_mut()
                        .receive_data(data.clone(), &mut self.ctx);
                } else {
                    self.topology
                        .borrow_mut()
                        .local_receive_data(data.clone(), &mut self.ctx)
                }
                self.ctx
                    .emit_now(DataTransferCompleted { data: data.clone() }, data.notification_dest);
            }
        })
    }
}
