//! Description of some structs required for describing topology network.

use crate::model::NetworkModel;
use std::collections::BTreeMap;

pub(crate) type NodeId = usize;
pub(crate) type LinkId = usize;
pub(crate) type NodeLinksMap = BTreeMap<NodeId, BTreeMap<NodeId, LinkId>>;

pub(crate) const INVALID_NODE_ID: usize = usize::MAX;

/// Type of the link.
#[derive(Copy, Clone, Debug)]
pub enum BandwidthSharingPolicy {
    /// Shares bandwidth equally between all transfers.
    Shared,
    /// Constant throughput for all transfers.
    NonShared,
}

/// A link between two nodes in the network.
#[derive(Copy, Clone, Debug)]
pub struct Link {
    pub(crate) bandwidth: f64,
    pub(crate) latency: f64,
    pub(crate) sharing_policy: BandwidthSharingPolicy,
}

impl Link {
    /// Creates new link with [shared](BandwidthSharingPolicy::Shared) sharing policy.
    pub fn shared(bandwidth: f64, latency: f64) -> Self {
        Self {
            bandwidth,
            latency,
            sharing_policy: BandwidthSharingPolicy::Shared,
        }
    }

    /// Creates new link with [non-shared](BandwidthSharingPolicy::NonShared) sharing policy.
    pub fn non_shared(bandwidth: f64, latency: f64) -> Self {
        Self {
            bandwidth,
            latency,
            sharing_policy: BandwidthSharingPolicy::NonShared,
        }
    }
}

/// A node in the network.
pub struct Node {
    pub(crate) local_network: Box<dyn NetworkModel>,
}
