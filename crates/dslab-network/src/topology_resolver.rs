use std::collections::{BTreeMap, HashMap, HashSet};

use crate::topology_structures::{Link, LinkID, LinksMap, Node};

pub struct TopologyResolver {
    parent_path: HashMap<String, HashMap<String, String>>,
}

impl TopologyResolver {
    pub fn new() -> TopologyResolver {
        return TopologyResolver {
            parent_path: HashMap::new(),
        };
    }

    pub fn update_latencies_for_node(&mut self, node: &str, links: &BTreeMap<LinkID, Link>, node_links_map: &LinksMap) {
        let mut latency: HashMap<String, f64> = HashMap::new();
        let mut parent: HashMap<String, String> = HashMap::new();
        for n in node_links_map.keys() {
            latency.insert(n.to_string(), f64::INFINITY);
        }
        latency.insert(node.to_string(), 0.0);
        let mut visited: HashSet<String> = HashSet::new();
        for _ in 0..node_links_map.len() {
            let mut relax_node = "";
            for next_node in node_links_map.keys() {
                if !visited.contains(next_node) && (relax_node == "" || latency[next_node] < latency[relax_node]) {
                    relax_node = &next_node;
                }
            }

            if latency[relax_node] == f64::INFINITY {
                break;
            }

            for (node_to, link_id) in node_links_map.get(relax_node).unwrap() {
                let link = links.get(link_id).unwrap();
                if latency[relax_node] + link.latency < latency[node_to] {
                    latency.insert(node_to.to_string(), latency[relax_node] + link.latency);
                    parent.insert(node_to.to_string(), relax_node.to_string());
                }
            }
            visited.insert(relax_node.to_string());
        }
        self.parent_path.insert(node.to_string(), parent);
    }

    pub fn resolve_topology(
        &mut self,
        nodes: &BTreeMap<String, Node>,
        links: &BTreeMap<LinkID, Link>,
        node_links_map: &LinksMap,
    ) {
        for node in nodes.keys() {
            self.update_latencies_for_node(node, &links, &node_links_map);
        }
    }

    pub fn get_path(&self, src: &str, dst: &str, node_links_map: &LinksMap) -> Option<Vec<LinkID>> {
        let mut path = Vec::new();
        let mut cur_node = dst.clone();
        while cur_node != src {
            if !self.parent_path[src].contains_key(cur_node) {
                return None;
            }
            let link_id = node_links_map[cur_node][&self.parent_path[src][cur_node]];
            path.push(link_id);
            cur_node = self.parent_path[src].get(cur_node).unwrap();
        }
        path.reverse();
        Some(path)
    }
}
