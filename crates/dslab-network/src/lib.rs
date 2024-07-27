//! A library for modeling network communications. It can be easily plugged in simulations based on DSLab core by
//! defining network nodes and binding the simulation components to the nodes. After that, using the library one can
//! simulate data transfers or messaging between the components. The library includes several ready-to-use network
//! models and allows to use it with custom model implementations.
//!
//! ## Network models
//!
//! - [`ConstantBandwidthNetworkModel`](crate::models::ConstantBandwidthNetworkModel): Simple topology-unaware model
//!   where each transfer gets the full network bandwidth, i.e. there is no contention.
//! - [`SharedBandwidthNetworkModel`](crate::models::SharedBandwidthNetworkModel): Topology-unaware model where the
//!   network bandwidth is shared fairly among all current transfers.
//! - [`TopologyAwareNetworkModel`](crate::models::TopologyAwareNetworkModel): Topology-aware model which uses
//!   information about the network [`Topology`] (links connecting the nodes) and relies on
//!   [`RoutingAlgorithm`](crate::routing::RoutingAlgorithm) to compute paths between the nodes. The link's bandwidth is
//!   shared fairly among the transfers using the link.
//!
//! ## Examples
//!
//! - [network-simple](https://github.com/osukhoroslov/dslab/tree/main/examples/network-simple): demonstrates the use of
//!   topology-unaware models.
//! - [network-topology](https://github.com/osukhoroslov/dslab/tree/main/examples/network-topology): demonstrates the
//!   use of [`TopologyAwareNetworkModel`](crate::models::TopologyAwareNetworkModel).
//! - [network-topology-benchmark](https://github.com/osukhoroslov/dslab/tree/main/examples/network-topology-benchmark):
//!   benchmarks the performance of [`TopologyAwareNetworkModel`](crate::models::TopologyAwareNetworkModel) on different
//!   topologies.

#![warn(missing_docs)]

pub mod link;
pub mod model;
pub mod models;
pub mod network;
pub mod node;
pub mod routing;
pub mod topology;

pub use link::{BandwidthSharingPolicy, Link, LinkId};
pub use model::{DataTransfer, DataTransferCompleted, NetworkModel};
pub use network::{Message, MessageDelivered, Network};
pub use node::{Node, NodeId};
pub use topology::Topology;
