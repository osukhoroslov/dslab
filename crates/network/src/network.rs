use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};

use log::debug;

use core::cast;
use core::context::SimulationContext;
use core::event::{Event, EventData};
use core::handler::EventHandler;

use crate::model::*;
use crate::shared_bandwidth_model::SharedBandwidthNetwork;

// pub const NETWORK_ID: &str = "net";

struct HostInfo {
    local_network: Box<dyn NetworkModel>,
}

pub struct Network {
    network_model: Rc<RefCell<dyn NetworkModel>>,
    hosts: BTreeMap<String, HostInfo>,
    actor_hosts: HashMap<String, String>,
    id_counter: AtomicUsize,
    ctx: SimulationContext,
}

impl Network {
    pub fn new(network_model: Rc<RefCell<dyn NetworkModel>>, ctx: SimulationContext) -> Self {
        Self {
            network_model,
            hosts: BTreeMap::new(),
            actor_hosts: HashMap::new(),
            id_counter: AtomicUsize::new(1),
            ctx,
        }
    }

    pub fn add_host(&mut self, host_id: &str, local_bandwidth: f64, local_latency: f64) {
        let local_network = SharedBandwidthNetwork::new(local_bandwidth, local_latency);
        self.hosts.insert(
            host_id.to_string(),
            HostInfo {
                local_network: Box::new(local_network),
            },
        );
    }

    pub fn set_location(&mut self, id: &str, host_id: &str) {
        self.actor_hosts.insert(id.to_string(), host_id.to_string());
    }

    pub fn get_location(&self, id: &str) -> Option<&String> {
        self.actor_hosts.get(id)
    }

    pub fn check_same_host(&self, id1: &str, id2: &str) -> bool {
        let host1 = self.get_location(id1);
        let host2 = self.get_location(id2);
        host1.is_some() && host2.is_some() && host1.unwrap() == host2.unwrap()
    }

    pub fn get_hosts(&self) -> Vec<String> {
        self.hosts.keys().cloned().collect()
    }

    pub fn send_msg<S: Into<String>>(&mut self, message: String, src: S, dest: S) -> usize {
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

    pub fn send_event<T: EventData, S: AsRef<str>>(&mut self, data: T, src: S, dest: S) {
        debug!(
            "System time: {}, {} send Event to {}",
            self.ctx.time(),
            src.as_ref(),
            dest.as_ref()
        );

        if !self.check_same_host(src.as_ref(), dest.as_ref()) {
            self.ctx
                .emit_as(data, src.as_ref(), dest.as_ref(), self.network_model.borrow().latency());
        } else {
            let hostname = self.get_location(src.as_ref()).unwrap();
            let local_latency = self.hosts.get(hostname).unwrap().local_network.latency();
            self.ctx.emit_as(data, src.as_ref(), dest.as_ref(), local_latency);
        }
    }

    pub fn transfer_data<S: Into<String>>(&mut self, src: S, dest: S, size: f64, notification_dest: S) -> usize {
        let data_id = self.id_counter.fetch_add(1, Ordering::Relaxed);
        let data = Data {
            id: data_id,
            src: src.into(),
            dest: dest.into(),
            size,
            notification_dest: notification_dest.into(),
        };
        self.ctx.emit_self_now(DataTransferRequest { data });
        data_id
    }
}

impl EventHandler for Network {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            MessageSend { message } => {
                debug!(
                    "System time: {}, {} send Message '{}' to {}",
                    self.ctx.time(),
                    message.src,
                    message.data,
                    message.dest.clone()
                );
                if !self.check_same_host(&message.src, &message.dest) {
                    let message_recieve_event = MessageReceive { message };
                    self.ctx
                        .emit_self(message_recieve_event, self.network_model.borrow().latency());
                } else {
                    let hostname = self.get_location(&message.dest).unwrap();
                    let local_latency = self.hosts.get(hostname).unwrap().local_network.latency();
                    let message_recieve_event = MessageReceive { message };
                    self.ctx.emit_self(message_recieve_event, local_latency);
                }
            }
            MessageReceive { message } => {
                debug!(
                    "System time: {}, {} received Message '{}' from {}",
                    self.ctx.time(),
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
                debug!(
                    "System time: {}, Data ID: {}, From: {}, To {}, Size: {}",
                    self.ctx.time(),
                    data.id,
                    data.src,
                    data.dest,
                    data.size
                );
                if !self.check_same_host(&data.src, &data.dest) {
                    self.ctx
                        .emit_self(StartDataTransfer { data }, self.network_model.borrow().latency());
                } else {
                    let hostname = self.get_location(&data.dest).unwrap();
                    let local_latency = self.hosts.get(hostname).unwrap().local_network.latency();
                    self.ctx.emit_self(StartDataTransfer { data }, local_latency);
                }
            }
            StartDataTransfer { data } => {
                if !self.check_same_host(&data.src, &data.dest) {
                    self.network_model.borrow_mut().send_data(data, &mut self.ctx);
                } else {
                    let hostname = self.get_location(&data.dest).unwrap().clone();
                    self.hosts
                        .get_mut(&hostname)
                        .unwrap()
                        .local_network
                        .send_data(data, &mut self.ctx);
                }
            }
            DataReceive { data } => {
                debug!(
                    "System time: {}, Data ID: {}, From: {}, To {}, Size: {}",
                    self.ctx.time(),
                    data.id,
                    data.src,
                    data.dest,
                    data.size
                );
                if !self.check_same_host(&data.src, &data.dest) {
                    self.network_model
                        .borrow_mut()
                        .receive_data(data.clone(), &mut self.ctx);
                } else {
                    let hostname = self.get_location(&data.dest).unwrap().clone();
                    self.hosts
                        .get_mut(&hostname)
                        .unwrap()
                        .local_network
                        .receive_data(data.clone(), &mut self.ctx);
                }
                self.ctx
                    .emit_now(DataTransferCompleted { data: data.clone() }, data.notification_dest);
            }
        })
    }
}
