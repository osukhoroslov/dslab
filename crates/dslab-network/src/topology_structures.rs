use crate::model::NetworkModel;
use std::collections::BTreeMap;

pub type NodeId = usize;
pub type LinkID = usize;
pub type NodeLinksMap = BTreeMap<NodeId, BTreeMap<NodeId, LinkID>>;

pub const INVALID_NODE_ID: usize = usize::MAX;

pub struct Link {
    pub latency: f64,
    pub bandwidth: f64,
}

impl Link {
    pub fn new(latency: f64, bandwidth: f64) -> Self {
        Self { latency, bandwidth }
    }
}

pub struct Node {
    pub local_network: Box<dyn NetworkModel>,
}
