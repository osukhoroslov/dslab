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
    id: u64,
    source: ActorId,
    dest: ActorId,
    size: u64,
    requester: ActorId,
}

struct HostInfo {}

pub struct Network {
    latency: f64,
    bandwidth: f64,
    hosts: BTreeMap<String, HostInfo>,
    actor_hosts: HashMap<ActorId, String>,
    transfers: BTreeMap<u64, DataTransferRequest>,
}

impl Network {
    pub fn new(latency: f64, bandwidth: f64) -> Self {
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

    pub fn place_actor(&mut self, actor_id: ActorId, host_id: &str) {
        self.actor_hosts.insert(actor_id, host_id.to_string());
    }

    pub fn get_hosts(&self) -> Vec<String> {
        self.hosts.keys().map(|v| v.clone()).collect()
    }

    pub fn send<T: Event>(&self, event: T, dest: ActorId, ctx: &mut ActorContext) {
        let source_host = self.actor_hosts.get(&ctx.id).unwrap();
        let dest_host = self.actor_hosts.get(&dest).unwrap();
        if source_host != dest_host {
            ctx.emit(event, dest, self.latency);
        } else {
            ctx.emit(event, dest, 0.);
        }
    }

    pub fn transfer(&self, id: u64, source: ActorId, dest: ActorId, size: u64, ctx: &mut ActorContext) {
        let source_host = self.actor_hosts.get(&source).unwrap();
        let dest_host = self.actor_hosts.get(&dest).unwrap();
        if source_host != dest_host {
            let req = DataTransferRequest {
                id,
                source,
                dest,
                size,
                requester: ctx.id.clone(),
            };
            ctx.emit_now(req, ActorId::from(NETWORK_ID));
        } else {
            ctx.emit_now(DataTransferCompleted { id }, ctx.id.clone());
        }
    }
}

impl Actor for Network {
    #[allow(unused_variables)]
    fn on(&mut self, event: Box<dyn Event>, _from: ActorId, ctx: &mut ActorContext) {
        cast!(match event {
            DataTransferRequest {
                id,
                source,
                dest,
                size,
                requester,
            } => {
                println!("{} [{}] data transfer started: {:?}", ctx.time(), ctx.id, event);
                let transfer_time = self.latency + (*size as f64 / self.bandwidth);
                ctx.emit_self(DataTransferCompleted { id: *id }, transfer_time);
                self.transfers
                    .insert(*id, *event.downcast::<DataTransferRequest>().unwrap());
            }
            DataTransferCompleted { id } => {
                let transfer = self.transfers.remove(id).unwrap();
                println!("{} [{}] data transfer completed: {:?}", ctx.time(), ctx.id, transfer);
                ctx.emit_now(DataTransferCompleted { id: *id }, transfer.requester);
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}
