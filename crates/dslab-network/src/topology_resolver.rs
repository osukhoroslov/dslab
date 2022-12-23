use std::collections::{BTreeMap, HashMap, HashSet};

use crate::topology_structures::{Link, LinkID, Node, NodeId, NodeLinksMap, INVALID_NODE_ID};

#[derive(PartialEq, Default)]
enum TopologyResolveType {
    Dijkstra,
    #[default]
    FloydWarshall,
}

#[derive(Default)]
pub struct TopologyResolver {
    resolve_type: TopologyResolveType,
    parent_path: Vec<Vec<NodeId>>,
}

impl TopologyResolver {
    pub fn new() -> TopologyResolver {
        Default::default()
    }

    pub fn resolve_topology(
        &mut self,
        nodes: &BTreeMap<NodeId, Node>,
        links: &BTreeMap<LinkID, Link>,
        node_links_map: &NodeLinksMap,
    ) {
        self.parent_path = vec![vec![INVALID_NODE_ID; nodes.len()]; nodes.len()];

        if self.resolve_type == TopologyResolveType::Dijkstra {
            for node in nodes.keys() {
                self.dijkstra_for_node(node, links, node_links_map);
            }
        }

        if self.resolve_type == TopologyResolveType::FloydWarshall {
            self.resolve_with_floyd_warshall(nodes, links, node_links_map)
        }
    }

    pub fn get_path(&self, src: &NodeId, dst: &NodeId, node_links_map: &NodeLinksMap) -> Option<Vec<LinkID>> {
        let mut path = Vec::new();
        let mut cur_node = *dst;
        while cur_node != *src {
            if self.parent_path[*src][cur_node] == INVALID_NODE_ID {
                return None;
            }
            let link_id = node_links_map[&cur_node][&self.parent_path[*src][cur_node]];
            path.push(link_id);
            cur_node = self.parent_path[*src][cur_node];
        }
        path.reverse();
        Some(path)
    }

    fn resolve_with_floyd_warshall(
        &mut self,
        nodes: &BTreeMap<NodeId, Node>,
        links: &BTreeMap<LinkID, Link>,
        node_links_map: &NodeLinksMap,
    ) {
        let mut current_paths = vec![vec![f64::INFINITY; nodes.len()]; nodes.len()];
        for node in nodes.keys() {
            current_paths[*node][*node] = 0.0;
            self.parent_path[*node][*node] = *node;
        }

        for (node1, intermap) in node_links_map {
            for (node2, link_id) in intermap {
                current_paths[*node1][*node2] = links.get(link_id).unwrap().latency;
                self.parent_path[*node1][*node2] = *node1;
            }
        }

        for k in 0..nodes.len() {
            for i in 0..nodes.len() {
                for j in 0..nodes.len() {
                    if current_paths[i][k] < f64::INFINITY
                        && current_paths[k][j] < f64::INFINITY
                        && current_paths[i][k] + current_paths[k][j] < current_paths[i][j]
                    {
                        current_paths[i][j] = current_paths[i][k] + current_paths[k][j];
                        let prev = self.parent_path[k][j];
                        self.parent_path[i][j] = prev;
                    }
                }
            }
        }
    }

    fn dijkstra_for_node(&mut self, node: &NodeId, links: &BTreeMap<LinkID, Link>, node_links_map: &NodeLinksMap) {
        let mut latency: HashMap<NodeId, f64> = HashMap::new();
        for n in node_links_map.keys() {
            latency.insert(*n, f64::INFINITY);
        }
        latency.insert(*node, 0.0);
        let mut visited: HashSet<NodeId> = HashSet::new();
        for _ in 0..node_links_map.len() {
            let mut relax_node = usize::MAX;
            for next_node in node_links_map.keys() {
                if !visited.contains(next_node)
                    && (relax_node == usize::MAX || latency[next_node] < latency[&relax_node])
                {
                    relax_node = *next_node;
                }
            }

            if latency[&relax_node] == f64::INFINITY {
                break;
            }

            for (node_to, link_id) in node_links_map.get(&relax_node).unwrap() {
                let link = links.get(link_id).unwrap();
                if latency[&relax_node] + link.latency < latency[node_to] {
                    latency.insert(*node_to, latency[&relax_node] + link.latency);
                    self.parent_path[*node][*node_to] = relax_node;
                }
            }
            visited.insert(relax_node);
        }
    }
}
