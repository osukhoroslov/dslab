use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap, HashSet, VecDeque};
use std::rc::Rc;

use dslab_core::component::Id;
use dslab_core::context::SimulationContext;

use crate::model::*;
use crate::topology::Topology;
use crate::topology_structures::{LinkID, NodeId};

#[derive(Debug)]
struct DataTransfer {}

#[derive(Clone)]
struct LinkUsage {
    link_id: usize,
    transfers_count: usize,
    left_bandwidth: f64,
}

impl LinkUsage {
    fn get_path_bandwidth(&self) -> f64 {
        self.left_bandwidth / self.transfers_count as f64
    }
}

impl Ord for LinkUsage {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.transfers_count == 0 || other.transfers_count == 0 {
            panic!("Invalid cmp for Link usage")
        }
        // sort order is reversed because LinkUsage is used in BinaryHeap,
        // which extracts maximum element, and we need link with minimum bandwidth.
        other
            .get_path_bandwidth()
            .total_cmp(&(self.get_path_bandwidth()))
            .then(other.link_id.cmp(&self.link_id))
    }
}

impl PartialOrd for LinkUsage {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for LinkUsage {
    fn eq(&self, other: &Self) -> bool {
        self.link_id == other.link_id
            && self.transfers_count == other.transfers_count
            && self.left_bandwidth == other.left_bandwidth
    }
}

impl Eq for LinkUsage {}

#[derive(Debug)]
struct Transfer {
    size_left: f64,
    last_update_time: f64,
    throughput: f64,
    data: Data,
    path: Vec<LinkID>,
}

impl Transfer {
    fn new(size: f64, data: Data, path: Vec<LinkID>, time: f64) -> Transfer {
        Transfer {
            size_left: size,
            data,
            throughput: 0.0,
            last_update_time: time,
            path,
        }
    }

    fn expected_time_left(&self) -> f64 {
        self.size_left / self.throughput
    }

    fn expected_finish(&self) -> f64 {
        self.last_update_time + self.expected_time_left()
    }
}

pub struct TopologyNetwork {
    topology: Rc<RefCell<Topology>>,
    current_transfers: BTreeMap<usize, Transfer>,
    transfers_through_link: Vec<Vec<usize>>,
    tmp_transfers_through_link: Vec<Vec<usize>>,
    next_event: Option<u64>,
    next_event_index: Option<usize>,
    link_data: Vec<Option<LinkUsage>>,
    full_mesh_optimization: bool,
}

impl TopologyNetwork {
    pub fn new(topology: Rc<RefCell<Topology>>) -> TopologyNetwork {
        TopologyNetwork {
            topology,
            current_transfers: BTreeMap::new(),
            transfers_through_link: Vec::new(),
            tmp_transfers_through_link: Vec::new(),
            next_event: None,
            next_event_index: None,
            link_data: Vec::new(),
            full_mesh_optimization: false,
        }
    }

    /// Enables optimization which greatly improves simulation times for cases with a lot of non-intersecting transfers.
    pub fn with_full_mesh_optimization(mut self, full_mesh_optimization: bool) -> Self {
        self.full_mesh_optimization = full_mesh_optimization;
        self
    }

    fn get_location(&self, node: Id) -> NodeId {
        let topology = self.topology.borrow();
        let node1 = topology.get_location(node);
        if node1.is_none() {
            panic!("Invalid host {}", node);
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

    /// Finds smallest subset of transfers which contains `updated_transfer` so that sets of links
    /// used by transfers inside and outside this subset don't intersect.
    fn get_affected_transfers(&self, updated_transfer: usize) -> HashSet<usize> {
        if self.current_transfers.is_empty() {
            return HashSet::new();
        }
        let limit = self.current_transfers[&updated_transfer].throughput;

        let mut processed_links = HashSet::new();
        let mut processed_transfers = HashSet::new();
        let mut q = VecDeque::new();
        q.push_back(updated_transfer);
        while let Some(transfer) = q.pop_front() {
            processed_transfers.insert(transfer);
            for &link in self.current_transfers[&transfer].path.iter() {
                if processed_links.contains(&link) {
                    continue;
                }
                processed_links.insert(link);
                for &t in self.transfers_through_link[link].iter() {
                    if self.current_transfers[&t].throughput < limit {
                        continue;
                    }
                    if !processed_transfers.contains(&t) {
                        processed_transfers.insert(t);
                        q.push_back(t);
                    }
                }
            }
        }
        processed_transfers
    }

    fn update_next_event(&mut self, ctx: &mut SimulationContext) {
        self.next_event_index = self
            .current_transfers
            .iter()
            .min_by(|x, y| x.1.expected_finish().total_cmp(&y.1.expected_finish()))
            .map(|(x, _y)| *x);

        if let Some(event_idx) = self.next_event_index {
            let transfer = &self.current_transfers[&event_idx];
            let time = transfer.expected_finish();
            self.next_event = Some(ctx.emit_self(
                DataReceive {
                    data: transfer.data.clone(),
                },
                time - ctx.time(),
            ));
        };
    }

    /// Updates throughput for all transfers from `affected_transfers`.
    fn calc(&mut self, ctx: &mut SimulationContext, affected_transfers: HashSet<usize>) {
        if affected_transfers.is_empty() {
            return;
        }

        let topology = self.topology.borrow();

        for transfer_id in affected_transfers.iter() {
            let transfer = self.current_transfers.get_mut(transfer_id).unwrap();
            transfer.size_left -= transfer.throughput * (ctx.time() - transfer.last_update_time);
            transfer.size_left = transfer.size_left.max(0.);
            transfer.last_update_time = ctx.time();
        }

        if let Some(event_id) = self.next_event {
            ctx.cancel_event(event_id)
        };

        let affected_links = affected_transfers
            .iter()
            .flat_map(|transfer| self.current_transfers[transfer].path.iter().cloned())
            .collect::<HashSet<LinkID>>();

        let transfers_through_link = &mut self.tmp_transfers_through_link;

        let mut current_link_usage: BinaryHeap<LinkUsage> = BinaryHeap::new();
        for (link_id, transfers) in affected_links
            .iter()
            .map(|link_id| (link_id, &self.transfers_through_link[*link_id]))
        {
            let cur_transfers: Vec<usize> = transfers
                .iter()
                .filter(|transfer| affected_transfers.contains(transfer))
                .cloned()
                .collect();
            if cur_transfers.is_empty() {
                continue;
            }
            let link = LinkUsage {
                link_id: *link_id,
                transfers_count: cur_transfers.len(),
                left_bandwidth: topology.get_link(link_id).unwrap().bandwidth,
            };
            transfers_through_link[*link_id] = cur_transfers;
            self.link_data[*link_id] = Some(link);
        }

        for (transfer_id, transfer) in self.current_transfers.iter() {
            if affected_transfers.contains(transfer_id) {
                continue;
            }
            for &link in transfer.path.iter() {
                if self.link_data[link].is_some() {
                    self.link_data[link].as_mut().unwrap().left_bandwidth -= transfer.throughput;
                }
            }
        }

        for &link in affected_links.iter() {
            if self.link_data[link].is_some() {
                current_link_usage.push(self.link_data[link].clone().unwrap());
            }
        }

        let mut assigned_transfer: HashSet<usize> = HashSet::new();

        let mut last_bandwidth = 0.0;
        while let Some(min_link) = current_link_usage.pop() {
            let min_link_id = min_link.link_id;
            if self.link_data[min_link_id].is_none() {
                // delayed removal
                continue;
            }
            if self.link_data[min_link_id].as_ref().unwrap() != &min_link {
                // delayed update
                current_link_usage.push(self.link_data[min_link_id].as_ref().unwrap().clone());
                continue;
            }

            let bandwidth = min_link.get_path_bandwidth();
            if bandwidth < last_bandwidth - 1e-12 {
                panic!("{:.20} < {:.20}", bandwidth, last_bandwidth);
            }
            let bandwidth = bandwidth.max(last_bandwidth);
            last_bandwidth = bandwidth;
            for &transfer_idx in transfers_through_link[min_link_id].iter() {
                if assigned_transfer.contains(&transfer_idx) {
                    continue;
                }
                assigned_transfer.insert(transfer_idx);
                self.current_transfers.get_mut(&transfer_idx).unwrap().throughput = bandwidth;
                for &link in self.current_transfers[&transfer_idx].path.iter() {
                    if link != min_link_id {
                        if self.link_data[link].as_ref().unwrap().transfers_count == 1 {
                            self.link_data[link] = None;
                            continue;
                        }
                        let mut link_usage = self.link_data[link].take().unwrap();
                        link_usage.transfers_count -= 1;
                        link_usage.left_bandwidth -= bandwidth;
                        self.link_data[link] = Some(link_usage);
                    }
                }
            }
            transfers_through_link[min_link_id].clear();
        }
    }

    /// Same as [TopologyNetwork::calc] with `affected_transfers` equal to the set of all transfers,
    /// but this corner case allows for some optimization.
    fn calc_all(&mut self, ctx: &mut SimulationContext) {
        let topology = self.topology.borrow();

        for transfer in self.current_transfers.values_mut() {
            transfer.size_left -= transfer.throughput * (ctx.time() - transfer.last_update_time);
            transfer.size_left = transfer.size_left.max(0.);
            transfer.last_update_time = ctx.time();
        }

        if let Some(event_id) = self.next_event {
            ctx.cancel_event(event_id)
        };

        let mut current_link_usage: BinaryHeap<LinkUsage> = BinaryHeap::new();
        for (link_id, transfers) in self.transfers_through_link.iter().enumerate() {
            if transfers.is_empty() {
                continue;
            }
            let link = LinkUsage {
                link_id,
                transfers_count: transfers.len(),
                left_bandwidth: topology.get_link(&link_id).unwrap().bandwidth,
            };
            current_link_usage.push(link.clone());
            self.link_data[link_id] = Some(link);
        }

        let mut assigned_transfer: HashSet<usize> = HashSet::new();

        let mut last_bandwidth = 0.0;
        while let Some(min_link) = current_link_usage.pop() {
            let min_link_id = min_link.link_id;
            if self.link_data[min_link_id].is_none() {
                // delayed removal
                continue;
            }
            if self.link_data[min_link_id].as_ref().unwrap() != &min_link {
                // delayed update
                current_link_usage.push(self.link_data[min_link_id].as_ref().unwrap().clone());
                continue;
            }

            let bandwidth = min_link.get_path_bandwidth();
            if bandwidth < last_bandwidth - 1e-12 {
                panic!("{:.20} < {:.20}", bandwidth, last_bandwidth);
            }
            let bandwidth = bandwidth.max(last_bandwidth);
            last_bandwidth = bandwidth;
            for &transfer_idx in self.transfers_through_link[min_link_id].iter() {
                if assigned_transfer.contains(&transfer_idx) {
                    continue;
                }
                assigned_transfer.insert(transfer_idx);
                self.current_transfers.get_mut(&transfer_idx).unwrap().throughput = bandwidth;
                for &link in self.current_transfers[&transfer_idx].path.iter() {
                    if link != min_link_id {
                        if self.link_data[link].as_ref().unwrap().transfers_count == 1 {
                            self.link_data[link] = None;
                            continue;
                        }
                        let mut link_usage = self.link_data[link].take().unwrap();
                        link_usage.transfers_count -= 1;
                        link_usage.left_bandwidth -= bandwidth;
                        self.link_data[link] = Some(link_usage);
                    }
                }
            }
        }
    }

    fn validate_array_lengths(&mut self) {
        let topology = self.topology.borrow();
        self.link_data.resize(topology.get_links_count(), None);
        self.transfers_through_link
            .resize(topology.get_links_count(), Vec::new());
        self.tmp_transfers_through_link
            .resize(topology.get_links_count(), Vec::new());
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
        if !self.check_path_exists(data.src, data.dest) {
            return;
        }
        self.validate_array_lengths();

        let path = self.get_path(data.src, data.dest).unwrap();
        let id = data.id;
        assert!(!self.current_transfers.contains_key(&data.id));
        for &link in path.iter() {
            self.transfers_through_link[link].push(id);
        }
        self.current_transfers
            .insert(id, Transfer::new(data.size, data, path, ctx.time()));

        if self.full_mesh_optimization {
            let affected_transfers = self.get_affected_transfers(id);
            self.calc(ctx, affected_transfers);
        } else {
            self.calc_all(ctx);
        }
        self.update_next_event(ctx);
    }

    fn receive_data(&mut self, _data: Data, ctx: &mut SimulationContext) {
        self.validate_array_lengths();
        let next_event_index = self.next_event_index.unwrap();
        let affected_transfers = if self.full_mesh_optimization {
            let mut transfers = self.get_affected_transfers(next_event_index);
            assert!(transfers.remove(&next_event_index));
            transfers
        } else {
            HashSet::new()
        };
        let transfer = self.current_transfers.remove(&next_event_index).unwrap();
        for &link in transfer.path.iter() {
            let vec = self.transfers_through_link.get_mut(link).unwrap();
            vec.remove(vec.iter().position(|&x| x == next_event_index).unwrap());
        }
        self.next_event = None;
        self.next_event_index = None;
        if self.full_mesh_optimization {
            self.calc(ctx, affected_transfers);
        } else {
            self.calc_all(ctx);
        }
        self.update_next_event(ctx);
    }

    fn recalculate_operations(&mut self, ctx: &mut SimulationContext) {
        self.validate_array_lengths();
        self.calc_all(ctx);
        self.update_next_event(ctx);
    }
}

impl NetworkModel for TopologyNetwork {}
