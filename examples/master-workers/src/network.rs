use core::actor::{Actor, ActorContext, ActorId, Event};
use core::cast;
use std::collections::{BTreeMap, HashMap};

pub const NETWORK_ID: &str = "net";

#[derive(Debug)]
pub struct DataTransferCompleted {
    pub id: u64,
}

#[derive(Debug)]
pub struct DataTransferRequest {
    source: ActorId,
    dest: ActorId,
    size: u64,
    requester: ActorId,
}

struct HostInfo {}

pub struct Network {
    latency: f64,
    bandwidth: u64,
    hosts: BTreeMap<String, HostInfo>,
    actor_hosts: HashMap<ActorId, String>,
    transfers: BTreeMap<u64, DataTransferRequest>,
}

impl Network {
    pub fn new(latency: f64, bandwidth: u64) -> Self {
        Self {
            latency,
            bandwidth,
            hosts: BTreeMap::new(),
            actor_hosts: HashMap::new(),
            transfers: BTreeMap::new(),
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
        self.hosts.keys().map(|v| v.clone()).collect()
    }

    pub fn send<T: Event>(&self, event: T, dest: ActorId, ctx: &mut ActorContext) {
        if !self.check_same_host(&ctx.id, &dest) {
            ctx.emit(event, dest, self.latency);
        } else {
            ctx.emit(event, dest, 0.);
        }
    }

    pub fn transfer(&self, source: ActorId, dest: ActorId, size: u64, ctx: &mut ActorContext) -> u64 {
        let req = DataTransferRequest {
            source,
            dest,
            size,
            requester: ctx.id.clone(),
        };
        ctx.emit_now(req, ActorId::from(NETWORK_ID))
    }
}

impl Actor for Network {
    #[allow(unused_variables)]
    fn on(&mut self, event: Box<dyn Event>, _from: ActorId, ctx: &mut ActorContext) {
        cast!(match event {
            DataTransferRequest {
                source,
                dest,
                size,
                requester,
            } => {
                let transfer_id = ctx.event_id;
                println!(
                    "{} [{}] data transfer {} started: {:?}",
                    ctx.time(),
                    ctx.id,
                    transfer_id,
                    event
                );
                let mut transfer_time = 0.;
                if !self.check_same_host(&source, &dest) {
                    transfer_time = self.latency + (*size as f64 / self.bandwidth as f64);
                }
                ctx.emit_self(DataTransferCompleted { id: transfer_id }, transfer_time);
                self.transfers
                    .insert(transfer_id, *event.downcast::<DataTransferRequest>().unwrap());
            }
            DataTransferCompleted { id } => {
                let transfer = self.transfers.remove(id).unwrap();
                println!(
                    "{} [{}] data transfer {} completed: {:?}",
                    ctx.time(),
                    ctx.id,
                    *id,
                    transfer
                );
                ctx.emit_now(DataTransferCompleted { id: *id }, transfer.requester);
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}
