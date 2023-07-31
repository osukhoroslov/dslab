//! Network topology.

use std::collections::BTreeMap;

use crate::{Link, LinkId, Node, NodeId};

/// Stores for each node a map with its neighbors and corresponding outgoing links.
pub type NodeLinksMap = BTreeMap<NodeId, BTreeMap<NodeId, LinkId>>;

/// Represents a network topology consisting of nodes connected with links.
#[derive(Default)]
pub struct Topology {
    nodes: Vec<Node>,
    links: Vec<Link>,
    node_links_map: NodeLinksMap,
}

impl Topology {
    /// Creates a new empty topology.
    pub fn new() -> Self {
        Default::default()
    }

    /// Adds a new node and returns the assigned id.
    pub fn add_node(&mut self, node: Node) -> NodeId {
        let node_id = self.nodes.len();
        self.nodes.push(node);
        self.node_links_map.insert(node_id, BTreeMap::new());
        node_id
    }

    /// Returns the number of nodes.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Adds a new bidirectional link between two nodes.
    pub fn add_link(&mut self, node1: NodeId, node2: NodeId, link: Link) -> LinkId {
        self.add_link_internal(node1, node2, link, true)
    }

    /// Adds a new unidirectional link between two nodes.
    pub fn add_unidirectional_link(&mut self, node_from: NodeId, node_to: NodeId, link: Link) -> LinkId {
        self.add_link_internal(node_from, node_to, link, false)
    }

    /// Adds two unidirectional links with the same parameters between two nodes in opposite directions.
    pub fn add_full_duplex_link(&mut self, node1: NodeId, node2: NodeId, link: Link) -> (LinkId, LinkId) {
        (
            self.add_link_internal(node1, node2, link, false),
            self.add_link_internal(node2, node1, link, false),
        )
    }

    /// Returns the link by its id.
    pub fn link(&self, link_id: LinkId) -> &Link {
        self.links
            .get(link_id)
            .unwrap_or_else(|| panic!("Link {} is not found", link_id))
    }

    /// Returns the number of links.
    pub fn link_count(&self) -> usize {
        self.links.len()
    }

    /// Returns an immutable reference to the stored [`NodeLinksMap`].
    pub fn node_links_map(&self) -> &NodeLinksMap {
        &self.node_links_map
    }

    /// Returns the network latency of the given path.
    pub fn get_path_latency(&self, path: &[LinkId]) -> f64 {
        let latency = path.iter().map(|link_id| self.link(*link_id).latency).sum();
        latency
    }

    /// Returns the network bandwidth of the given path.
    pub fn get_path_bandwidth(&self, path: &[LinkId]) -> f64 {
        let bandwidth = path
            .iter()
            .map(|link_id| self.link(*link_id).bandwidth)
            .min_by(|a, b| a.total_cmp(b))
            .unwrap();
        bandwidth
    }

    fn add_link_internal(&mut self, node1: NodeId, node2: NodeId, link: Link, bidirectional: bool) -> LinkId {
        assert!(link.bandwidth > 0.0, "Link bandwidth must be > 0");
        let link_id = self.links.len();
        self.links.push(link);
        self.node_links_map.get_mut(&node1).unwrap().insert(node2, link_id);
        if bidirectional {
            self.node_links_map.get_mut(&node2).unwrap().insert(node1, link_id);
        }
        link_id
    }
}
