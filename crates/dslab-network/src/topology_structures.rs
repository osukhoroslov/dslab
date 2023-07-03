use crate::model::NetworkModel;
use std::collections::BTreeMap;

pub type NodeId = usize;
pub type LinkID = usize;
pub type NodeLinksMap = BTreeMap<NodeId, BTreeMap<NodeId, LinkID>>;

pub const INVALID_NODE_ID: usize = usize::MAX;

#[derive(Copy, Clone, Debug)]
pub enum BandwidthSharingPolicy {
    Shared,
    NonShared,
}

#[derive(Copy, Clone, Debug)]
pub struct Link {
    pub bandwidth: f64,
    pub latency: f64,
    pub link_type: BandwidthSharingPolicy,
}

impl Link {
    pub fn shared(bandwidth: f64, latency: f64) -> Self {
        Self {
            bandwidth,
            latency,
            link_type: BandwidthSharingPolicy::Shared,
        }
    }

    pub fn non_shared(bandwidth: f64, latency: f64) -> Self {
        Self {
            bandwidth,
            latency,
            link_type: BandwidthSharingPolicy::NonShared,
        }
    }
}

pub struct Node {
    pub local_network: Box<dyn NetworkModel>,
}
