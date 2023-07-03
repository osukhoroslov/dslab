use std::collections::{BTreeMap, HashMap};

use indexmap::IndexMap;

use dslab_core::component::Id;
use dslab_core::context::SimulationContext;

use crate::model::*;
use crate::topology_resolver::TopologyResolveType;
use crate::topology_resolver::TopologyResolver;
use crate::topology_structures::{Link, LinkID, Node, NodeId, NodeLinksMap};

#[derive(Default)]
pub struct Topology {
    nodes_name_map: IndexMap<String, NodeId>,
    nodes: Vec<Node>,
    links: Vec<Link>,
    component_nodes: HashMap<Id, NodeId>,
    node_links_map: NodeLinksMap,
    resolver: Option<TopologyResolver>,
    bandwidth_cache: HashMap<(NodeId, NodeId), f64>,
    latency_cache: HashMap<(NodeId, NodeId), f64>,
    path_cache: HashMap<(NodeId, NodeId), Vec<LinkID>>,
    resolve_type: TopologyResolveType,
}

impl Topology {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn with_resolve_type(mut self, resolve_type: TopologyResolveType) -> Self {
        self.resolve_type = resolve_type;
        self
    }

    fn get_node_id(&self, node_name: &str) -> usize {
        if let Some(node_id) = self.nodes_name_map.get(node_name) {
            return *node_id;
        }
        panic!("Node with name {} doesn't exists", node_name)
    }

    pub fn add_node(&mut self, node_name: &str, local_network: Box<dyn NetworkModel>) {
        let new_node_id = self.nodes.len();
        self.nodes_name_map.insert(node_name.to_string(), new_node_id);
        self.nodes.push(Node { local_network });
        self.node_links_map.insert(new_node_id, BTreeMap::new());
    }

    fn add_link_internal(&mut self, node1_name: &str, node2_name: &str, link: Link, bidirectional: bool) {
        assert!(link.bandwidth > 0.0, "Link bandwidth must be > 0");
        let node1 = self.get_node_id(node1_name);
        let node2 = self.get_node_id(node2_name);
        self.check_node_exists(node1);
        self.check_node_exists(node2);
        let link_id = self.links.len();
        self.links.push(link);
        self.node_links_map.get_mut(&node1).unwrap().insert(node2, link_id);
        if bidirectional {
            self.node_links_map.get_mut(&node2).unwrap().insert(node1, link_id);
        }
    }

    pub fn add_link(&mut self, node1_name: &str, node2_name: &str, link: Link) {
        self.add_link_internal(node1_name, node2_name, link, true);
        self.on_topology_change();
    }

    pub fn add_unidirectional_link(&mut self, node1_name: &str, node2_name: &str, link: Link) {
        self.add_link_internal(node1_name, node2_name, link, false);
        self.on_topology_change();
    }

    pub fn add_full_duplex_link(&mut self, node1_name: &str, node2_name: &str, link: Link) {
        self.add_link_internal(node1_name, node2_name, link, false);
        self.add_link_internal(node2_name, node1_name, link, false);
        self.on_topology_change();
    }

    fn on_topology_change(&mut self) {
        self.bandwidth_cache.clear();
        self.latency_cache.clear();
        self.path_cache.clear();
        self.resolve_topology();
    }

    fn resolve_topology(&mut self) {
        match &mut self.resolver {
            None => (),
            Some(resolver) => {
                resolver.resolve_topology(&self.nodes, &self.links, &self.node_links_map);
            }
        }
    }

    // Init topology resolver to perform calculations
    pub fn init(&mut self) {
        self.resolver = Some(TopologyResolver::new(self.resolve_type));
        self.resolve_topology();
    }

    pub fn set_location(&mut self, id: Id, node_name: &str) {
        let node_id = self.get_node_id(node_name);
        self.component_nodes.insert(id, node_id);
    }

    pub fn get_location(&self, id: Id) -> Option<&NodeId> {
        self.component_nodes.get(&id)
    }

    pub fn check_same_node(&self, id1: Id, id2: Id) -> bool {
        let node1 = self.get_location(id1);
        let node2 = self.get_location(id2);
        node1.is_some() && node2.is_some() && node1.unwrap() == node2.unwrap()
    }

    pub fn get_nodes_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn get_nodes(&self) -> Vec<String> {
        self.nodes_name_map.keys().cloned().collect()
    }

    pub fn get_node_info(&self, id: &NodeId) -> Option<&Node> {
        self.nodes.get(*id)
    }

    pub fn get_node_info_mut(&mut self, id: &NodeId) -> Option<&mut Node> {
        self.nodes.get_mut(*id)
    }

    pub fn local_receive_data(&mut self, data: Data, ctx: &mut SimulationContext) {
        let node = *self.get_location(data.dest).unwrap();
        self.get_node_info_mut(&node)
            .unwrap()
            .local_network
            .receive_data(data, ctx)
    }

    pub fn local_send_data(&mut self, data: Data, ctx: &mut SimulationContext) {
        let node = *self.get_location(data.dest).unwrap();
        self.get_node_info_mut(&node)
            .unwrap()
            .local_network
            .send_data(data, ctx)
    }

    pub fn get_local_latency(&self, src: Id, dst: Id) -> f64 {
        let node = self.get_location(src).unwrap();
        self.get_node_info(node).unwrap().local_network.latency(src, dst)
    }

    pub fn get_link(&self, link_id: &LinkID) -> Option<&Link> {
        self.links.get(*link_id)
    }

    pub fn get_link_between(&self, src: &NodeId, dst: &NodeId) -> Option<&Link> {
        let link_id = self.node_links_map.get(src).unwrap().get(dst);
        match link_id {
            None => None,
            Some(link_id) => self.links.get(*link_id),
        }
    }

    pub fn get_node_links_map(&self) -> &NodeLinksMap {
        &self.node_links_map
    }

    pub fn get_path(&mut self, src: &NodeId, dst: &NodeId) -> Option<Vec<LinkID>> {
        if let Some(path) = self.path_cache.get(&(*src, *dst)) {
            return Some(path.clone());
        }

        if let Some(resolver) = &self.resolver {
            let path_opt = resolver.get_path(src, dst, &self.node_links_map);

            if let Some(path) = path_opt {
                self.path_cache.insert((*src, *dst), path.clone());
                return Some(path);
            }
        }

        None
    }

    pub fn get_latency(&mut self, src: &NodeId, dst: &NodeId) -> f64 {
        if let Some(latency) = self.latency_cache.get(&(*src, *dst)) {
            return *latency;
        }

        if let Some(path) = self.get_path(src, dst) {
            let mut latency = 0.0;
            for link_id in &path {
                latency += self.get_link(link_id).unwrap().latency;
            }
            self.latency_cache.insert((*src, *dst), latency);
            self.latency_cache.insert((*dst, *src), latency);
            return latency;
        }
        f64::INFINITY
    }

    pub fn get_bandwidth(&mut self, src: &NodeId, dst: &NodeId) -> f64 {
        if let Some(bandwidth) = self.bandwidth_cache.get(&(*src, *dst)) {
            return *bandwidth;
        }

        if let Some(path) = self.get_path(src, dst) {
            let min_bandwidth_link = path
                .into_iter()
                .min_by(|x, y| {
                    self.get_link(x)
                        .unwrap()
                        .bandwidth
                        .partial_cmp(&self.get_link(y).unwrap().bandwidth)
                        .unwrap()
                })
                .unwrap();
            let bandwidth = self.get_link(&min_bandwidth_link).unwrap().bandwidth;

            self.bandwidth_cache.insert((*src, *dst), bandwidth);
            self.bandwidth_cache.insert((*dst, *src), bandwidth);

            return bandwidth;
        }
        0.0
    }

    pub fn get_links_count(&self) -> usize {
        self.links.len()
    }

    fn check_node_exists(&self, node_id: NodeId) {
        assert!(node_id < self.nodes.len())
    }
}
