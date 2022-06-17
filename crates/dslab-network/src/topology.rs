use std::collections::{BTreeMap, HashMap};

use dslab_core::component::Id;
use dslab_core::context::SimulationContext;

use crate::model::*;
use crate::shared_bandwidth_model::SharedBandwidthNetwork;

pub struct Node {
    pub local_network: Box<dyn NetworkModel>,
}

pub struct Topology {
    nodes: BTreeMap<String, Node>,
    component_nodes: HashMap<Id, String>,
}

impl Topology {
    pub fn new() -> Self {
        Self {
            nodes: BTreeMap::new(),
            component_nodes: HashMap::new(),
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

    pub fn get_local_latency(&mut self, src: Id, dst: Id) -> f64 {
        let node = self.get_location(src).unwrap();
        self.get_node_info(node).unwrap().local_network.latency(src, dst)
    }
}
