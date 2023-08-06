//! Network model implementations.

pub mod constant;
pub mod shared;
pub mod topology_aware;

pub use constant::ConstantBandwidthNetworkModel;
pub use shared::SharedBandwidthNetworkModel;
pub use topology_aware::TopologyAwareNetworkModel;
