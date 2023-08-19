//! Simulation component representing a network.

use indexmap::IndexMap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde::Serialize;

use dslab_core::component::Id;
use dslab_core::context::SimulationContext;
use dslab_core::event::{Event, EventData, EventId};
use dslab_core::handler::EventHandler;
use dslab_core::{cast, log_debug};

use crate::{DataTransfer, DataTransferCompleted, Link, LinkId, NetworkModel, Node, NodeId};

/// Represents a message sent between two simulation components over the network.
#[derive(Clone, Serialize)]
pub struct Message {
    /// Unique message id.
    pub id: usize,
    /// Simulation component which is sending the message.
    pub src: Id,
    /// Simulation component which is receiving the message.
    pub dst: Id,
    /// Contents of the message.
    pub data: String,
}

/// Event signalling the message delivery.
#[derive(Clone, Serialize)]
pub struct MessageDelivered {
    /// Delivered message.
    pub msg: Message,
}

#[derive(Clone, Serialize)]
struct StartDataTransfer {
    dt: DataTransfer,
}

/// Simulation component representing a network.
///
/// This is the main entry point for all network operations, which relies internally on the supplied network model.
pub struct Network {
    nodes_name_map: IndexMap<String, NodeId>,
    network_model: Box<dyn NetworkModel>,
    local_models: HashMap<NodeId, Box<dyn NetworkModel>>,
    locations: HashMap<Id, NodeId>,
    next_dt_id: AtomicUsize,
    next_msg_id: AtomicUsize,
    topology_initialized: bool,
    ctx: SimulationContext,
}

impl Network {
    /// Creates a new network with supplied network model.
    pub fn new(model: Box<dyn NetworkModel>, ctx: SimulationContext) -> Self {
        Self {
            nodes_name_map: IndexMap::new(),
            network_model: model,
            local_models: HashMap::new(),
            locations: HashMap::new(),
            next_dt_id: AtomicUsize::new(0),
            next_msg_id: AtomicUsize::new(0),
            topology_initialized: false,
            ctx,
        }
    }

    /// Returns component id in the simulation.
    pub fn id(&self) -> Id {
        self.ctx.id()
    }

    // Topology --------------------------------------------------------------------------------------------------------

    /// Adds a new network node.
    ///
    /// The supplied `local_model` is used to model the intra-node communications.
    pub fn add_node<S>(&mut self, name: S, local_model: Box<dyn NetworkModel>) -> NodeId
    where
        S: Into<String>,
    {
        let name = name.into();
        let node_id = if self.network_model.is_topology_aware() {
            self.network_model
                .topology_mut()
                .unwrap()
                .add_node(Node { name: name.clone() })
        } else {
            self.nodes_name_map.len()
        };
        self.nodes_name_map.insert(name, node_id);
        self.local_models.insert(node_id, local_model);
        node_id
    }

    /// Returns the node id by its name.
    pub fn get_node_id(&self, node: &str) -> usize {
        *self
            .nodes_name_map
            .get(node)
            .unwrap_or_else(|| panic!("Node {} is not found", node))
    }

    /// Returns the list of network nodes.
    pub fn get_nodes(&self) -> Vec<String> {
        self.nodes_name_map.keys().cloned().collect()
    }

    /// Adds a new bidirectional link between two nodes.
    pub fn add_link(&mut self, node1: &str, node2: &str, link: Link) -> LinkId {
        assert!(
            self.network_model.is_topology_aware(),
            "This method requires topology-aware model"
        );
        let node1 = self.get_node_id(node1);
        let node2 = self.get_node_id(node2);
        let link_id = self.network_model.topology_mut().unwrap().add_link(node1, node2, link);
        if self.topology_initialized {
            self.network_model.on_topology_change(&mut self.ctx);
        }
        link_id
    }

    /// Adds a new unidirectional link between two nodes.
    pub fn add_unidirectional_link(&mut self, node_from: &str, node_to: &str, link: Link) -> LinkId {
        assert!(
            self.network_model.is_topology_aware(),
            "This method requires topology-aware model"
        );
        let node_from = self.get_node_id(node_from);
        let node_to = self.get_node_id(node_to);
        let link_id = self
            .network_model
            .topology_mut()
            .unwrap()
            .add_unidirectional_link(node_from, node_to, link);
        if self.topology_initialized {
            self.network_model.on_topology_change(&mut self.ctx);
        }
        link_id
    }

    /// Adds two unidirectional links with the same parameters between two nodes in opposite directions.
    ///
    /// This allows to model the full-duplex communication links.
    pub fn add_full_duplex_link(&mut self, node1: &str, node2: &str, link: Link) -> (LinkId, LinkId) {
        assert!(
            self.network_model.is_topology_aware(),
            "This method requires topology-aware model"
        );
        let node1 = self.get_node_id(node1);
        let node2 = self.get_node_id(node2);
        let (uplink_id, downlink_id) = self
            .network_model
            .topology_mut()
            .unwrap()
            .add_full_duplex_link(node1, node2, link);
        if self.topology_initialized {
            self.network_model.on_topology_change(&mut self.ctx);
        }
        (uplink_id, downlink_id)
    }

    /// Performs initialization of network topology, such as computing the paths between the nodes.
    ///
    /// Must be called after all links are added and before submitting any operations.
    pub fn init_topology(&mut self) {
        assert!(
            self.network_model.is_topology_aware(),
            "This method requires topology-aware model"
        );
        self.network_model.on_topology_change(&mut self.ctx);
        self.topology_initialized = true;
    }

    // Component location ----------------------------------------------------------------------------------------------

    /// Sets the location of the simulation component `id` to the node `node`.
    pub fn set_location(&mut self, id: Id, node: &str) {
        self.locations.insert(id, self.get_node_id(node));
    }

    /// Returns the location (node id) of the simulation component if it is set.
    pub fn get_location_opt(&self, id: Id) -> Option<NodeId> {
        self.locations.get(&id).cloned()
    }

    /// Returns the location (node id) of the simulation component.
    ///
    /// Panics if the component location is not set.
    pub fn get_location(&self, id: Id) -> NodeId {
        self.get_location_opt(id)
            .unwrap_or_else(|| panic!("Component {} has unknown location", id))
    }

    // Bandwidth and latency -------------------------------------------------------------------------------------------

    /// Returns the network bandwidth between two simulation components.
    pub fn bandwidth(&self, src: Id, dst: Id) -> f64 {
        let src_node_id = self.get_location(src);
        let dst_node_id = self.get_location(dst);
        if src_node_id == dst_node_id {
            self.local_models[&src_node_id].bandwidth(src_node_id, src_node_id)
        } else {
            self.network_model.bandwidth(src_node_id, dst_node_id)
        }
    }

    /// Returns the network latency between two simulation components.
    pub fn latency(&self, src: Id, dst: Id) -> f64 {
        let src_node_id = self.get_location(src);
        let dst_node_id = self.get_location(dst);
        if src_node_id == dst_node_id {
            self.local_models[&src_node_id].latency(src_node_id, src_node_id)
        } else {
            self.network_model.latency(src_node_id, dst_node_id)
        }
    }

    // Operations ------------------------------------------------------------------------------------------------------

    /// Starts a data transfer between two simulation components, returns unique transfer id.
    ///
    /// The network locations of these components must be previously registered via [`Self::set_location`].
    /// The transfer completion time is calculated by the underlying network model.
    /// The [`DataTransferCompleted`] event is sent to `notification_dst` on the transfer completion.
    pub fn transfer_data(&mut self, src: Id, dst: Id, size: f64, notification_dst: Id) -> usize {
        let src_node_id = self.get_location(src);
        let dst_node_id = self.get_location(dst);
        let transfer_id = self.next_dt_id.fetch_add(1, Ordering::Relaxed);
        let dt = DataTransfer {
            id: transfer_id,
            src,
            src_node_id,
            dst,
            dst_node_id,
            size,
            notification_dst,
        };
        log_debug!(
            self.ctx,
            "new data transfer {} from {} to {} of size {}",
            dt.id,
            dt.src,
            dt.dst,
            dt.size
        );
        // The fixed part of data transfer time (latency) is modeled by the delayed StartDataTransfer event.
        // The remaining part is calculated by the underlying network model (see handling of StartDataTransfer event).
        let delay = self.latency(src, dst);
        self.ctx.emit_self(StartDataTransfer { dt }, delay);
        transfer_id
    }

    /// Sends a message between two simulation components, returns unique message id.
    ///
    /// The network locations of these components must be previously registered via [`Self::set_location`].
    /// The message delivery time is equal to the network latency, assuming the message data has a small size.
    /// The [`MessageDelivered`] event is sent to `dst` on the message delivery.
    pub fn send_msg(&mut self, message: String, src: Id, dst: Id) -> usize {
        log_debug!(self.ctx, "{} sent message '{}' to {}", src, message, dst);
        let msg_id = self.next_msg_id.fetch_add(1, Ordering::Relaxed);
        let msg = Message {
            id: msg_id,
            src,
            dst,
            data: message,
        };
        let delay = self.latency(src, dst);
        self.ctx.emit(MessageDelivered { msg }, dst, delay);
        msg_id
    }

    /// Sends an event between two simulation components, returns unique event id.
    ///
    /// The network locations of these components must be previously registered via [`Self::set_location`].
    /// The event delivery time is equal to the network latency, assuming the event data has a small size.
    pub fn send_event<T: EventData>(&mut self, data: T, src: Id, dst: Id) -> EventId {
        log_debug!(self.ctx, "{} sent event to {}", src, dst);
        let delay = self.latency(src, dst);
        self.ctx.emit_as(data, src, dst, delay)
    }
}

impl EventHandler for Network {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            StartDataTransfer { dt } => {
                let model = if dt.src_node_id == dt.dst_node_id {
                    self.local_models.get_mut(&dt.src_node_id).unwrap()
                } else {
                    &mut self.network_model
                };
                model.start_transfer(dt, &mut self.ctx);
            }
            DataTransferCompleted { dt } => {
                log_debug!(
                    self.ctx,
                    "completed data transfer {} from {} to {} of size {}",
                    dt.id,
                    dt.src,
                    dt.dst,
                    dt.size
                );
                let model = if dt.src_node_id == dt.dst_node_id {
                    self.local_models.get_mut(&dt.src_node_id).unwrap()
                } else {
                    &mut self.network_model
                };
                model.on_transfer_completion(dt.clone(), &mut self.ctx);
                let notification_dst = dt.notification_dst;
                self.ctx.emit_now(DataTransferCompleted { dt }, notification_dst);
            }
        })
    }
}
