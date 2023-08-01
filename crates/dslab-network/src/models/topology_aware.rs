//! Topology-aware network model.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap, HashSet, VecDeque};

use dslab_core::context::SimulationContext;

use crate::routing::{RoutingAlgorithm, ShortestPathFloydWarshall};
use crate::{BandwidthSharingPolicy, DataTransfer, DataTransferCompleted, LinkId, NetworkModel, NodeId, Topology};

// Link usage ----------------------------------------------------------------------------------------------------------

#[derive(Clone)]
struct LinkUsage {
    link_id: usize,
    transfers_count: usize,
    left_bandwidth: f64,
    sharing_policy: BandwidthSharingPolicy,
}

impl LinkUsage {
    fn get_path_bandwidth(&self) -> f64 {
        match self.sharing_policy {
            BandwidthSharingPolicy::Shared => self.left_bandwidth / self.transfers_count as f64,
            BandwidthSharingPolicy::NonShared => self.left_bandwidth,
        }
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

// Transfer info -------------------------------------------------------------------------------------------------------

#[derive(Debug)]
struct TransferInfo {
    dt: DataTransfer,
    path: Vec<LinkId>,
    size_left: f64,
    throughput: f64,
    last_update_time: f64,
}

impl TransferInfo {
    fn new(dt: DataTransfer, path: Vec<LinkId>, time: f64) -> TransferInfo {
        let size = dt.size;
        TransferInfo {
            dt,
            path,
            size_left: size,
            throughput: 0.0,
            last_update_time: time,
        }
    }

    fn expected_time_left(&self) -> f64 {
        self.size_left / self.throughput
    }

    fn expected_finish(&self) -> f64 {
        self.last_update_time + self.expected_time_left()
    }
}

// Model ---------------------------------------------------------------------------------------------------------------

/// Topology-aware network model supporting arbitrary network topologies.
pub struct TopologyAwareNetworkModel {
    topology: Topology,
    routing: Box<dyn RoutingAlgorithm>,
    current_transfers: BTreeMap<usize, TransferInfo>,
    transfers_through_link: Vec<Vec<usize>>,
    tmp_transfers_through_link: Vec<Vec<usize>>,
    next_event: Option<u64>,
    next_event_index: Option<usize>,
    link_data: Vec<Option<LinkUsage>>,
    full_mesh_optimization: bool,
}

#[allow(clippy::derivable_impls)]
impl Default for TopologyAwareNetworkModel {
    fn default() -> Self {
        TopologyAwareNetworkModel {
            topology: Topology::default(),
            routing: Box::<ShortestPathFloydWarshall>::default(),
            current_transfers: BTreeMap::new(),
            transfers_through_link: Vec::new(),
            tmp_transfers_through_link: Vec::new(),
            next_event: None,
            next_event_index: None,
            link_data: Vec::new(),
            full_mesh_optimization: false,
        }
    }
}

impl TopologyAwareNetworkModel {
    /// Creates a new network model.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enables optimization which greatly improves simulation times
    /// for cases with a lot of non-intersecting data transfers.
    pub fn with_full_mesh_optimization(mut self, full_mesh_optimization: bool) -> Self {
        self.full_mesh_optimization = full_mesh_optimization;
        self
    }

    /// Finds the smallest subset of transfers which contains `updated_transfer`
    /// so that the sets of links used by transfers inside and outside this subset don't intersect.
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
                DataTransferCompleted {
                    dt: transfer.dt.clone(),
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

        let topology = &self.topology;

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
            .collect::<HashSet<LinkId>>();

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
                left_bandwidth: topology.link(*link_id).bandwidth,
                sharing_policy: topology.link(*link_id).sharing_policy,
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
                        match link_usage.sharing_policy {
                            BandwidthSharingPolicy::Shared => link_usage.left_bandwidth -= bandwidth,
                            BandwidthSharingPolicy::NonShared => {}
                        }
                        self.link_data[link] = Some(link_usage);
                    }
                }
            }
            transfers_through_link[min_link_id].clear();
        }
    }

    /// Same as [`Self::calc`] with `affected_transfers` equal to the set of all transfers,
    /// but this corner case allows for some optimization.
    fn calc_all(&mut self, ctx: &mut SimulationContext) {
        let topology = &self.topology;

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
                left_bandwidth: topology.link(link_id).bandwidth,
                sharing_policy: topology.link(link_id).sharing_policy,
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
                        match link_usage.sharing_policy {
                            BandwidthSharingPolicy::Shared => link_usage.left_bandwidth -= bandwidth,
                            BandwidthSharingPolicy::NonShared => {}
                        }
                        self.link_data[link] = Some(link_usage);
                    }
                }
            }
        }
    }

    fn validate_array_lengths(&mut self) {
        let topology = &self.topology;
        self.link_data.resize(topology.link_count(), None);
        self.transfers_through_link.resize(topology.link_count(), Vec::new());
        self.tmp_transfers_through_link
            .resize(topology.link_count(), Vec::new());
    }
}

impl NetworkModel for TopologyAwareNetworkModel {
    fn is_topology_aware(&self) -> bool {
        true
    }

    fn init(&mut self, routing: Box<dyn RoutingAlgorithm>) {
        self.routing = routing;
        self.routing.init(&self.topology);
    }

    fn bandwidth(&self, src: NodeId, dst: NodeId) -> f64 {
        self.topology
            .get_path_bandwidth(self.routing.get_path_iter(src, dst, &self.topology).unwrap())
    }

    fn latency(&self, src: NodeId, dst: NodeId) -> f64 {
        self.topology
            .get_path_latency(self.routing.get_path_iter(src, dst, &self.topology).unwrap())
    }

    fn start_transfer(&mut self, dt: DataTransfer, ctx: &mut SimulationContext) {
        self.validate_array_lengths();
        let path = self
            .routing
            .get_path_iter(dt.src_node_id, dt.dst_node_id, &self.topology)
            .unwrap()
            .collect::<Vec<_>>();
        let id = dt.id;
        assert!(!self.current_transfers.contains_key(&dt.id));
        for &link in path.iter() {
            self.transfers_through_link[link].push(id);
        }
        self.current_transfers
            .insert(id, TransferInfo::new(dt, path, ctx.time()));

        if self.full_mesh_optimization {
            let affected_transfers = self.get_affected_transfers(id);
            self.calc(ctx, affected_transfers);
        } else {
            self.calc_all(ctx);
        }
        self.update_next_event(ctx);
    }

    fn on_transfer_completion(&mut self, _dt: DataTransfer, ctx: &mut SimulationContext) {
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

    fn topology(&self) -> Option<&Topology> {
        Some(&self.topology)
    }

    fn topology_mut(&mut self) -> Option<&mut Topology> {
        Some(&mut self.topology)
    }

    fn on_topology_change(&mut self, ctx: &mut SimulationContext) {
        self.routing.init(&self.topology);
        self.validate_array_lengths();
        self.calc_all(ctx);
        self.update_next_event(ctx);
    }
}
