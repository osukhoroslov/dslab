use priority_queue::DoublePriorityQueue;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::rc::Rc;

use dslab_core::component::Id;
use dslab_core::context::SimulationContext;

use crate::model::*;
use crate::topology::Topology;
use crate::topology_structures::{LinkID, NodeId};

#[derive(Debug)]
struct DataTransfer {}

pub struct LinkUsage {
    pub link_id: usize,
    pub transfers_count: usize,
    pub left_bandwidth: f64,
}

impl LinkUsage {
    pub fn get_path_bandwidth(&self) -> f64 {
        return self.left_bandwidth / self.transfers_count as f64;
    }
}

impl Ord for LinkUsage {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.transfers_count == 0 || other.transfers_count == 0 {
            panic!("Invalid cmp for Link usage")
        }
        self.get_path_bandwidth()
            .total_cmp(&(other.get_path_bandwidth()))
            .then(self.link_id.cmp(&other.link_id))
    }
}

impl PartialOrd for LinkUsage {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for LinkUsage {
    fn eq(&self, other: &Self) -> bool {
        return self.link_id == other.link_id;
    }
}

impl Eq for LinkUsage {}

pub struct Transfer {
    pub size_left: f64,
    pub last_update_time: f64,
    pub throughput: f64,
    pub data: Data,
}

impl Transfer {
    pub fn new(size: f64, data: Data) -> Transfer {
        return Transfer {
            size_left: size,
            data,
            throughput: 0.0,
            last_update_time: 0.0,
        };
    }
}

pub struct TopologyNetwork {
    topology: Rc<RefCell<Topology>>,
    current_transfers: Vec<Transfer>,
    next_event: Option<u64>,
    next_event_index: Option<usize>,
}

impl TopologyNetwork {
    pub fn new(topology: Rc<RefCell<Topology>>) -> TopologyNetwork {
        return TopologyNetwork {
            topology,
            current_transfers: Vec::new(),
            next_event: None,
            next_event_index: None,
        };
    }

    fn get_location(&self, node: Id) -> NodeId {
        let topology = self.topology.borrow();
        let node1 = topology.get_location(node);
        if node1.is_none() {
            panic!("Invalid host")
        }
        *node1.unwrap()
    }

    fn get_path(&self, from: Id, to: Id) -> Option<Vec<LinkID>> {
        let node1 = self.get_location(from);
        let node2 = self.get_location(to);
        self.topology.borrow_mut().get_path(&node1, &node2)
    }

    fn check_path_exists(&self, src: Id, dst: Id) -> bool {
        self.get_path(src, dst).is_some()
    }

    fn calculate_transfers(&mut self) {
        if self.current_transfers.len() == 0 {
            return;
        }

        let mut paths = Vec::new();

        for transfer_idx in 0..self.current_transfers.len() {
            let src = self.current_transfers[transfer_idx].data.src;
            let dst = self.current_transfers[transfer_idx].data.dest;
            let path = self.get_path(src, dst);
            if path.is_none() {
                panic!("Transfer with no path")
            }
            paths.push(path.unwrap());
        }

        let mut link_data = HashMap::new();
        let mut link_paths = HashMap::new();
        // Init initial link data
        for (idx, path) in paths.iter().enumerate() {
            for link_id in path {
                if !link_data.contains_key(link_id) {
                    let link_bandwidth = self.topology.borrow().get_link(link_id).unwrap().bandwidth;
                    link_data.insert(
                        *link_id,
                        LinkUsage {
                            link_id: *link_id,
                            transfers_count: 0,
                            left_bandwidth: link_bandwidth,
                        },
                    );
                    link_paths.insert(*link_id, Vec::new());
                }
                link_paths.get_mut(&link_id).unwrap().push(idx);
                link_data.get_mut(&link_id).unwrap().transfers_count += 1;
            }
        }

        let mut current_link_usage = DoublePriorityQueue::new();
        // Init current link uisage
        for (link_id, link_usage) in link_data {
            current_link_usage.push(link_id, link_usage);
        }

        let mut assigned_path = vec![false; paths.len()];
        // Calculate transfer bandwidths
        while current_link_usage.len() != 0 {
            let (link_with_minimal_bandwidth_id, link_with_minimal_bandwidth_usage) =
                current_link_usage.pop_min().unwrap();
            let mut links_decrease_paths = HashMap::new();
            let bandwidth = link_with_minimal_bandwidth_usage.get_path_bandwidth();
            for path_idx in link_paths.get(&link_with_minimal_bandwidth_id).unwrap() {
                if assigned_path[*path_idx] {
                    continue;
                }
                assigned_path[*path_idx] = true;
                self.current_transfers[*path_idx].throughput = bandwidth;
                for link in &paths[*path_idx] {
                    if !links_decrease_paths.contains_key(link) {
                        links_decrease_paths.insert(*link, 0);
                    }
                    *links_decrease_paths.get_mut(&link).unwrap() += 1;
                }
            }
            for (link_id, uses_amount) in links_decrease_paths {
                if link_id != link_with_minimal_bandwidth_id {
                    if current_link_usage.get(&link_id).unwrap().1.transfers_count == uses_amount {
                        current_link_usage.remove(&link_id);
                        continue;
                    }
                    current_link_usage.change_priority_by(&link_id, |link_usage: &mut LinkUsage| {
                        link_usage.transfers_count -= uses_amount;
                        link_usage.left_bandwidth -= bandwidth * uses_amount as f64;
                    });
                }
            }
        }

        let (next_event_index, _next_event) = self
            .current_transfers
            .iter()
            .enumerate()
            .min_by(|x, y| (x.1.size_left / x.1.throughput).total_cmp(&(y.1.size_left / y.1.throughput)))
            .unwrap();

        self.next_event_index = Some(next_event_index);
    }

    fn recalculate_receive_time(&mut self, ctx: &mut SimulationContext) {
        for transfer in &mut self.current_transfers {
            transfer.size_left -= transfer.throughput * (ctx.time() - transfer.last_update_time);
            transfer.last_update_time = ctx.time();
        }

        if let Some(event_id) = self.next_event {
            ctx.cancel_event(event_id)
        };

        self.calculate_transfers();

        if let Some(event_idx) = self.next_event_index {
            let transfer = self.current_transfers.get(event_idx).unwrap();
            let time = transfer.size_left / transfer.throughput;
            self.next_event = Some(ctx.emit_self(
                DataReceive {
                    data: transfer.data.clone(),
                },
                time,
            ));
        };
    }
}

impl NetworkConfiguration for TopologyNetwork {
    fn latency(&self, host1: Id, host2: Id) -> f64 {
        let node1 = self.get_location(host1);
        let node2 = self.get_location(host2);
        self.topology.borrow_mut().get_latency(&node1, &node2)
    }

    fn bandwidth(&self, host1: Id, host2: Id) -> f64 {
        let node1 = self.get_location(host1);
        let node2 = self.get_location(host2);
        self.topology.borrow_mut().get_bandwidth(&node1, &node2)
    }
}

impl DataOperation for TopologyNetwork {
    fn send_data(&mut self, data: Data, ctx: &mut SimulationContext) {
        if self.check_path_exists(data.src, data.dest) {
            self.current_transfers.push(Transfer::new(data.size.clone(), data));
            self.recalculate_receive_time(ctx);
        }
    }

    fn receive_data(&mut self, _data: Data, ctx: &mut SimulationContext) {
        self.current_transfers.remove(self.next_event_index.unwrap());
        self.next_event = None;
        self.next_event_index = None;
        self.recalculate_receive_time(ctx);
    }

    fn recalculate_operations(&mut self, ctx: &mut SimulationContext) {
        self.recalculate_receive_time(ctx);
    }
}

impl NetworkModel for TopologyNetwork {}
