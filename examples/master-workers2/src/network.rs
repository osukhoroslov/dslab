use log::trace;
use std::collections::{BTreeMap, HashMap};

use core2::cast;
use core2::context::SimulationContext;
use core2::event::{Event, EventData};
use core2::handler::EventHandler;

#[derive(Debug)]
pub struct DataTransferCompleted {
    pub id: u64,
}

#[derive(Debug)]
pub struct DataTransferRequest {
    source: String,
    dest: String,
    size: u64,
    requester: String,
}

struct HostInfo {}

pub struct Network {
    id: String,
    latency: f64,
    bandwidth: u64,
    hosts: BTreeMap<String, HostInfo>,
    locations: HashMap<String, String>,
    transfers: BTreeMap<u64, DataTransferRequest>,
    next_id: u64,
    ctx: SimulationContext,
}

impl Network {
    pub fn new(latency: f64, bandwidth: u64, ctx: SimulationContext) -> Self {
        Self {
            id: ctx.id().to_string(),
            latency,
            bandwidth,
            hosts: BTreeMap::new(),
            locations: HashMap::new(),
            transfers: BTreeMap::new(),
            next_id: 0,
            ctx,
        }
    }

    pub fn add_host(&mut self, host_id: &str) {
        self.hosts.insert(host_id.to_string(), HostInfo {});
    }

    pub fn set_location(&mut self, id: &str, host_id: &str) {
        self.locations.insert(id.to_string(), host_id.to_string());
    }

    pub fn get_location(&self, id: &str) -> Option<&String> {
        self.locations.get(id)
    }

    pub fn check_same_host(&self, id1: &str, id2: &str) -> bool {
        let host1 = self.get_location(id1);
        let host2 = self.get_location(id2);
        host1.is_some() && host2.is_some() && host1.unwrap() == host2.unwrap()
    }

    pub fn get_hosts(&self) -> Vec<String> {
        self.hosts.keys().cloned().collect()
    }

    pub fn send<T: EventData, S: AsRef<str>>(&mut self, event: T, src: S, dest: S) {
        if !self.check_same_host(src.as_ref(), dest.as_ref()) {
            self.ctx.emit_as(event, src.as_ref(), dest.as_ref(), self.latency);
        } else {
            self.ctx.emit_as(event, src.as_ref(), dest.as_ref(), 0.);
        }
    }

    pub fn transfer<S: Into<String>>(&mut self, source: S, dest: S, size: u64, requester: S) -> u64 {
        let req = DataTransferRequest {
            source: source.into(),
            dest: dest.into(),
            size,
            requester: requester.into(),
        };
        let transfer_id = self.next_id;
        self.next_id += 1;
        trace!(
            "{} [{}] data transfer {} started: {:?}",
            self.ctx.time(),
            self.id,
            transfer_id,
            req
        );
        let mut transfer_time = 0.;
        if !self.check_same_host(&req.source, &req.dest) {
            transfer_time = self.latency + (req.size as f64 / self.bandwidth as f64);
        }
        self.ctx
            .emit_self(DataTransferCompleted { id: transfer_id }, transfer_time);
        self.transfers.insert(transfer_id, req);
        transfer_id
    }

    fn on_transfer_completed(&mut self, transfer_id: u64) {
        let transfer = self.transfers.remove(&transfer_id).unwrap();
        trace!(
            "{} [{}] data transfer {} completed: {:?}",
            self.ctx.time(),
            self.id,
            transfer_id,
            transfer
        );
        self.ctx
            .emit_now(DataTransferCompleted { id: transfer_id }, transfer.requester);
    }
}

impl EventHandler for Network {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            DataTransferCompleted { id } => {
                self.on_transfer_completed(*id);
            }
        })
    }
}
