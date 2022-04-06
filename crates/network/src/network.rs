use std::cell::RefCell;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};

use simcore::component::Id;
use simcore::context::SimulationContext;
use simcore::event::{Event, EventData};
use simcore::handler::EventHandler;
use simcore::{cast, log_debug};

use crate::model::*;
use crate::topology::Topology;

// pub const NETWORK_ID: &str = "net";

pub struct Network {
    network_model: Rc<RefCell<dyn NetworkModel>>,
    topology: Topology,
    id_counter: AtomicUsize,
    ctx: SimulationContext,
}

impl Network {
    pub fn new(network_model: Rc<RefCell<dyn NetworkModel>>, ctx: SimulationContext) -> Self {
        Self {
            network_model,
            topology: Topology::new(),
            id_counter: AtomicUsize::new(1),
            ctx,
        }
    }

    pub fn add_node(&mut self, node_id: &str, local_bandwidth: f64, local_latency: f64) {
        self.topology.add_node(node_id, local_bandwidth, local_latency)
    }

    pub fn set_location(&mut self, id: Id, node_id: &str) {
        self.topology.set_location(id, node_id)
    }

    pub fn get_location(&self, id: Id) -> Option<&String> {
        self.topology.get_location(id)
    }

    pub fn check_same_node(&self, id1: Id, id2: Id) -> bool {
        self.topology.check_same_node(id1, id2)
    }

    pub fn get_nodes(&self) -> Vec<String> {
        self.topology.get_nodes()
    }

    pub fn send_msg(&mut self, message: String, src: Id, dest: Id) -> usize {
        let msg_id = self.id_counter.fetch_add(1, Ordering::Relaxed);
        let msg = Message {
            id: msg_id,
            src: src.into(),
            dest: dest.into(),
            data: message,
        };
        self.ctx.emit_self_now(MessageSend { message: msg });
        msg_id
    }

    pub fn send_event<T: EventData>(&mut self, data: T, src: Id, dest: Id) {
        log_debug!(self.ctx, "{} sent event to {}", src, dest);

        let latency = if self.check_same_node(src, dest) {
            self.topology.get_local_latency(src, dest)
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
                    self.topology.get_local_latency(message.src, message.dest)
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
                    self.topology.get_local_latency(data.src, data.dest)
                } else {
                    self.network_model.borrow().latency(data.src, data.dest)
                };
                self.ctx.emit_self(StartDataTransfer { data }, latency);
            }
            StartDataTransfer { data } => {
                if !self.check_same_node(data.src, data.dest) {
                    self.network_model.borrow_mut().send_data(data, &mut self.ctx);
                } else {
                    self.topology.local_send_data(data, &mut self.ctx)
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
                    self.topology.local_receive_data(data.clone(), &mut self.ctx)
                }
                self.ctx
                    .emit_now(DataTransferCompleted { data: data.clone() }, data.notification_dest);
            }
        })
    }
}
