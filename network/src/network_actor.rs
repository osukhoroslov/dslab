use log::info;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};

use core::actor::{Actor, ActorContext, ActorId, Event};
use core::match_event;
use core::sim::Simulation;

use crate::model::*;

pub const NETWORK_ID: &str = "net";

struct HostInfo {}

pub struct NetworkActor {
    network_model: Rc<RefCell<dyn NetworkModel>>,
    hosts: BTreeMap<String, HostInfo>,
    actor_hosts: HashMap<ActorId, String>,
    latency: f64,

    id_counter: AtomicUsize,
}

impl NetworkActor {
    pub fn new(network_model: Rc<RefCell<dyn NetworkModel>>, latency: f64) -> Self {
        Self {
            network_model,
            hosts: BTreeMap::new(),
            actor_hosts: HashMap::new(),
            latency,
            id_counter: AtomicUsize::new(1),
        }
    }

    pub fn add_host(&mut self, host_id: &str) {
        self.hosts.insert(host_id.to_string(), HostInfo {});
    }

    pub fn set_actor_host(&mut self, actor_id: ActorId, host_id: &str) {
        self.actor_hosts.insert(actor_id, host_id.to_string());
    }

    pub fn get_actor_host(&self, actor_id: &ActorId) -> Option<&String> {
        self.actor_hosts.get(&actor_id)
    }

    pub fn check_same_host(&self, actor1_id: &ActorId, actor2_id: &ActorId) -> bool {
        let actor1_host = self.get_actor_host(&actor1_id);
        let actor2_host = self.get_actor_host(&actor2_id);
        actor1_host.is_some() && actor2_host.is_some() && actor1_host.unwrap() == actor2_host.unwrap()
    }

    pub fn get_hosts(&self) -> Vec<String> {
        self.hosts.keys().cloned().collect()
    }

    fn send<T: Event>(&self, event: T, dest: ActorId, ctx: &mut ActorContext) {
        if !self.check_same_host(&ctx.id, &dest) {
            ctx.emit(event, dest, self.latency);
        } else {
            ctx.emit(event, dest, 0.);
        }
    }

    pub fn send_msg(&self, message: String, dest: ActorId, ctx: &mut ActorContext) -> usize {
        let msg_id = self.id_counter.fetch_add(1, Ordering::Relaxed);
        let msg = Message {
            id: msg_id,
            source: ctx.id.clone(),
            dest: dest.clone(),
            data: message,
        };
        self.send(MessageSend { message: msg }, ActorId::from(NETWORK_ID), ctx);

        msg_id
    }

    pub fn transfer_data(&self, source: ActorId, dest: ActorId, size: f64, ctx: &mut ActorContext) -> usize {
        let data_id = self.id_counter.fetch_add(1, Ordering::Relaxed);
        let data = Data {
            id: data_id,
            source,
            dest,
            size,
        };

        self.send(DataTransferRequest { data }, ActorId::from(NETWORK_ID), ctx);

        data_id
    }

    pub fn transfer_data_from_sim(&self, source: ActorId, dest: ActorId, size: f64, sim: &mut Simulation) -> usize {
        let data_id = self.id_counter.fetch_add(1, Ordering::Relaxed);
        let data = Data {
            id: data_id,
            source,
            dest,
            size,
        };

        sim.add_event(
            DataTransferRequest { data },
            ActorId::from(NETWORK_ID),
            ActorId::from(NETWORK_ID),
            self.latency,
        );

        data_id
    }

    pub fn send_message_from_sim(&self, message: String, dest: ActorId, sim: &mut Simulation) -> usize {
        let msg_id = self.id_counter.fetch_add(1, Ordering::Relaxed);
        let msg = Message {
            id: msg_id,
            source: ActorId::from(NETWORK_ID),
            dest: dest.clone(),
            data: message,
        };

        sim.add_event(
            MessageSend { message: msg },
            ActorId::from(NETWORK_ID),
            ActorId::from(NETWORK_ID),
            self.latency,
        );

        msg_id
    }
}

impl Actor for NetworkActor {
    fn on(&mut self, event: Box<dyn Event>, _from: ActorId, ctx: &mut ActorContext) {
        match_event!( event {
            MessageSend { message } => {
                info!("System time: {}, {} send Message '{}' to {}", ctx.time(), message.source, message.data, message.dest);
                ctx.emit(MessageReceive { message: message.clone() }, ctx.id.clone(), 0.0);
            },
            MessageReceive { message } => {
                info!("System time: {}, {} received Message '{}' from {}", ctx.time(), message.dest, message.data, message.source);
                ctx.emit(MessageDelivery {message: message.clone()}, message.dest.clone(), 0.0);
            },
            DataTransferRequest { data } => {
                self.network_model.borrow_mut().send_data(data.clone(), ctx);
            },
            DataReceive { data } => {
                self.network_model.borrow_mut().receive_data( data.clone(), ctx );
                ctx.emit(DataDelivery {data: data.clone()}, data.dest.clone(), 0.0);
            },
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}
