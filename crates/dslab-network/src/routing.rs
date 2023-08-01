//! Routing algorithms.

use std::collections::{HashMap, HashSet};

use crate::topology::NodeLinksMap;
use crate::{LinkId, NodeId, Topology};

const INVALID_NODE_ID: usize = usize::MAX;

/// Calculates the paths between pairs of nodes in a network.
pub trait RoutingAlgorithm {
    /// Performs initialization of the routing algorithm based on the provided network topology.
    fn init(&mut self, topology: &Topology);

    /// Returns a path iterator from node `src` to node `dst`.
    ///
    /// Can be used only after calling [`Self::init`].
    fn get_path_iter<'a>(&'a self, src: NodeId, dst: NodeId, topology: &'a Topology) -> Option<PathIterator<'a>>;
}

/// Iterator which returns links on a path.
pub struct PathIterator<'a> {
    src: NodeId,
    dst: NodeId,
    node_links_map: &'a NodeLinksMap,
    parent_path: &'a Vec<Vec<NodeId>>,
}

impl Iterator for PathIterator<'_> {
    type Item = LinkId;

    fn next(&mut self) -> Option<Self::Item> {
        if self.src == self.dst {
            return None;
        }
        let next = self.parent_path[self.dst][self.src];
        let link_id = self.node_links_map[&self.src][&next];
        self.src = next;
        Some(link_id)
    }
}

// Shortest Path (Floyd–Warshall) --------------------------------------------------------------------------------------

/// Static routing algorithm which returns shortest paths (by latency) computed using the Floyd–Warshall algorithm.
#[derive(Default)]
pub struct ShortestPathFloydWarshall {
    parent_path: Vec<Vec<NodeId>>,
}

impl RoutingAlgorithm for ShortestPathFloydWarshall {
    fn init(&mut self, topology: &Topology) {
        let node_count = topology.node_count();
        self.parent_path = vec![vec![INVALID_NODE_ID; node_count]; node_count];
        let mut current_paths = vec![vec![f64::INFINITY; node_count]; node_count];
        #[allow(clippy::needless_range_loop)]
        for node in 0..node_count {
            current_paths[node][node] = 0.0;
            self.parent_path[node][node] = node;
        }

        for (node1, intermap) in topology.inv_node_links_map() {
            for (node2, link_id) in intermap {
                current_paths[*node1][*node2] = topology.link(*link_id).latency;
                self.parent_path[*node1][*node2] = *node1;
            }
        }

        for k in 0..node_count {
            for i in 0..node_count {
                for j in 0..node_count {
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

    fn get_path_iter<'a>(&'a self, src: NodeId, dst: NodeId, topology: &'a Topology) -> Option<PathIterator<'a>> {
        if self.parent_path[dst][src] == INVALID_NODE_ID {
            None
        } else {
            Some(PathIterator {
                src,
                dst,
                node_links_map: topology.node_links_map(),
                parent_path: &self.parent_path,
            })
        }
    }
}

// Shortest Path (Dijkstra) --------------------------------------------------------------------------------------------

/// Static routing algorithm which returns shortest paths (by latency) computed using the Dijkstra's algorithm.
#[derive(Default)]
pub struct ShortestPathDijkstra {
    parent_path: Vec<Vec<NodeId>>,
}

impl ShortestPathDijkstra {
    fn dijkstra_for_node(&mut self, node: NodeId, topology: &Topology) {
        let node_links_map = topology.inv_node_links_map();
        let mut latency: HashMap<NodeId, f64> = HashMap::new();
        for n in node_links_map.keys() {
            latency.insert(*n, f64::INFINITY);
        }
        latency.insert(node, 0.0);
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
                let link = topology.link(*link_id);
                if latency[&relax_node] + link.latency < latency[node_to] {
                    latency.insert(*node_to, latency[&relax_node] + link.latency);
                    self.parent_path[node][*node_to] = relax_node;
                }
            }
            visited.insert(relax_node);
        }
    }
}

impl RoutingAlgorithm for ShortestPathDijkstra {
    fn init(&mut self, topology: &Topology) {
        let node_count = topology.node_count();
        self.parent_path = vec![vec![INVALID_NODE_ID; node_count]; node_count];
        for node in 0..node_count {
            self.dijkstra_for_node(node, topology);
        }
    }

    fn get_path_iter<'a>(&'a self, src: NodeId, dst: NodeId, topology: &'a Topology) -> Option<PathIterator<'a>> {
        if self.parent_path[dst][src] == INVALID_NODE_ID {
            None
        } else {
            Some(PathIterator {
                src,
                dst,
                node_links_map: topology.node_links_map(),
                parent_path: &self.parent_path,
            })
        }
    }
}
