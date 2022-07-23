use std::collections::{BTreeMap, HashMap};

use dslab_core::component::Id;
use dslab_core::context::SimulationContext;

use crate::model::*;
use crate::shared_bandwidth_model::SharedBandwidthNetwork;
use crate::topology_resolver::TopologyResolver;
use crate::topology_structures::{Link, LinkID, LinksMap, Node};

pub struct Topology {
    link_id_counter: usize,
    nodes: BTreeMap<String, Node>,
    links: BTreeMap<LinkID, Link>,
    component_nodes: HashMap<Id, String>,
    node_links_map: LinksMap,
    resolver: Option<TopologyResolver>,
    bandwidth_cache: HashMap<String, HashMap<String, f64>>,
    latency_cache: HashMap<String, HashMap<String, f64>>,
    path_cache: HashMap<String, HashMap<String, Vec<LinkID>>>,
}

impl Topology {
    pub fn new() -> Self {
        Self {
            link_id_counter: 1,
            nodes: BTreeMap::new(),
            links: BTreeMap::new(),
            component_nodes: HashMap::new(),
            node_links_map: BTreeMap::new(),
            resolver: None,
            bandwidth_cache: HashMap::new(),
            latency_cache: HashMap::new(),
            path_cache: HashMap::new(),
        }
    }

    pub fn add_node(&mut self, node_id: &str, local_bandwidth: f64, local_latency: f64) {
        let local_network = SharedBandwidthNetwork::new(local_bandwidth, local_latency);
        self.nodes.insert(
            node_id.to_string(),
            Node {
                local_network: Box::new(local_network),
            },
        );
        self.node_links_map.insert(node_id.to_string(), BTreeMap::new());
    }

    pub fn add_link(&mut self, node1: &str, node2: &str, latency: f64, bandwidth: f64) {
        assert!(bandwidth > 0.0, "Link bandwidth must be > 0");
        self.check_node_exists(node1);
        self.check_node_exists(node2);
        let link_id = self.link_id_counter;
        self.link_id_counter += 1;
        self.links.insert(link_id, Link::new(latency, bandwidth));
        self.node_links_map
            .get_mut(node1)
            .unwrap()
            .insert(node2.to_string(), link_id);
        self.node_links_map
            .get_mut(node2)
            .unwrap()
            .insert(node1.to_string(), link_id);
        self.on_topology_change();
    }

    fn on_topology_change(&mut self) {
        self.bandwidth_cache.clear();
        self.latency_cache.clear();
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
        self.resolver = Some(TopologyResolver::new());
        self.resolve_topology();
    }

    pub fn set_location(&mut self, id: Id, node_id: &str) {
        self.component_nodes.insert(id, node_id.to_string());
    }

    pub fn get_location(&self, id: Id) -> Option<&String> {
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
        self.nodes.keys().cloned().collect()
    }

    pub fn get_node_info(&self, id: &String) -> Option<&Node> {
        return self.nodes.get(id);
    }

    pub fn get_node_info_mut(&mut self, id: &String) -> Option<&mut Node> {
        return self.nodes.get_mut(id);
    }

    pub fn local_receive_data(&mut self, data: Data, ctx: &mut SimulationContext) {
        let node = self.get_location(data.dest).unwrap().clone();
        self.get_node_info_mut(&node)
            .unwrap()
            .local_network
            .receive_data(data, ctx)
    }

    pub fn local_send_data(&mut self, data: Data, ctx: &mut SimulationContext) {
        let node = self.get_location(data.dest).unwrap().clone();
        self.get_node_info_mut(&node)
            .unwrap()
            .local_network
            .send_data(data, ctx)
    }

    pub fn get_local_latency(&self, src: Id, dst: Id) -> f64 {
        let node = self.get_location(src).unwrap();
        self.get_node_info(&node).unwrap().local_network.latency(src, dst)
    }

    pub fn get_link(&self, link_id: &LinkID) -> Option<&Link> {
        self.links.get(link_id)
    }

    pub fn get_link_between(&self, src: &str, dst: &str) -> Option<&Link> {
        let link_id = self.node_links_map.get(src).unwrap().get(dst);
        match link_id {
            None => None,
            Some(link_id) => self.links.get(link_id),
        }
    }

    pub fn get_node_links_map(&self) -> &LinksMap {
        &self.node_links_map
    }

    pub fn get_path(&mut self, src: &str, dst: &str) -> Option<Vec<LinkID>> {
        if let Some(nested_map) = self.path_cache.get(src) {
            if let Some(path) = nested_map.get(dst) {
                return Some(path.clone());
            }
        }

        if let Some(nested_map) = self.path_cache.get(dst) {
            if let Some(path) = nested_map.get(src) {
                return Some(path.clone());
            }
        }

        if let Some(resolver) = &self.resolver {
            let path_opt = resolver.get_path(src, dst, &self.node_links_map);

            if let Some(path) = path_opt {
                if !self.path_cache.contains_key(src) {
                    self.path_cache.insert(src.to_string(), HashMap::new());
                }
                self.path_cache
                    .get_mut(src)
                    .unwrap()
                    .insert(dst.to_string(), path.clone());

                return Some(path);
            }
        }

        None
    }

    pub fn get_latency(&mut self, src: &str, dst: &str) -> f64 {
        if let Some(nested_map) = self.latency_cache.get(src) {
            if let Some(latency) = nested_map.get(dst) {
                return *latency;
            }
        }
        if let Some(nested_map) = self.latency_cache.get(dst) {
            if let Some(latency) = nested_map.get(src) {
                return *latency;
            }
        }

        if let Some(path) = self.get_path(src, dst) {
            let mut latency = 0.0;
            for link_id in &path {
                latency += self.get_link(link_id).unwrap().latency;
            }

            if !self.latency_cache.contains_key(src) {
                self.latency_cache.insert(src.to_string(), HashMap::new());
            }
            self.latency_cache
                .get_mut(src)
                .unwrap()
                .insert(dst.to_string(), latency);

            return latency;
        }
        return f64::INFINITY;
    }

    pub fn get_bandwidth(&mut self, src: &str, dst: &str) -> f64 {
        if let Some(nested_map) = self.bandwidth_cache.get(src) {
            if let Some(bandwidth) = nested_map.get(dst) {
                return *bandwidth;
            }
        }

        if let Some(nested_map) = self.bandwidth_cache.get(dst) {
            if let Some(bandwidth) = nested_map.get(src) {
                return *bandwidth;
            }
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

            if !self.bandwidth_cache.contains_key(src) {
                self.bandwidth_cache.insert(src.to_string(), HashMap::new());
            }
            self.bandwidth_cache
                .get_mut(src)
                .unwrap()
                .insert(dst.to_string(), bandwidth);

            return bandwidth;
        }
        return 0.0;
    }

    fn check_node_exists(&self, node_id: &str) {
        assert!(self.nodes.contains_key(node_id))
    }
}
