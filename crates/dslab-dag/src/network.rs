//! Network model tools.

use std::cell::RefCell;
use std::path::Path;
use std::rc::Rc;

use serde::{Deserialize, Serialize};

use dslab_core::context::SimulationContext;
use dslab_core::Id;
use dslab_network::models::{ConstantBandwidthNetworkModel, SharedBandwidthNetworkModel, TopologyAwareNetworkModel};
use dslab_network::{Link, Network};

use crate::resource::Resource;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum TopologyType {
    #[serde(rename = "star")]
    Star,
    #[serde(rename = "full_mesh")]
    FullMesh,
}

/// Represents network model parameters.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "model")]
pub enum NetworkConfig {
    ConstantBandwidth {
        /// Network bandwidth in MB/s.
        bandwidth: f64,
        /// Network latency in μs.
        latency: f64,
    },
    SharedBandwidth {
        /// Network bandwidth in MB/s.
        bandwidth: f64,
        /// Network latency in μs.
        latency: f64,
    },
    TopologyAware {
        #[serde(rename = "topology")]
        topology_type: TopologyType,
        /// Links bandwidth in MB/s.
        link_bandwidth: f64,
        /// Links latency in μs.
        link_latency: f64,
    },
}

impl NetworkConfig {
    /// Creates network config with [`ConstantBandwidthNetworkModel`].
    ///
    /// Bandwidth should be in MB/s, latency in μs.
    pub fn constant(bandwidth: f64, latency: f64) -> Self {
        NetworkConfig::ConstantBandwidth { bandwidth, latency }
    }

    /// Creates network config with [`SharedBandwidthNetworkModel`].
    ///
    /// Bandwidth should be in MB/s, latency in μs.
    pub fn shared(bandwidth: f64, latency: f64) -> Self {
        NetworkConfig::SharedBandwidth { bandwidth, latency }
    }

    /// Creates network config with [`TopologyAwareNetworkModel`].
    ///
    /// Bandwidth should be in MB/s, latency in μs.
    pub fn topology(topology_type: TopologyType, link_bandwidth: f64, link_latency: f64) -> Self {
        NetworkConfig::TopologyAware {
            topology_type,
            link_bandwidth,
            link_latency,
        }
    }

    /// Creates network model based on stored parameters.
    pub fn make_network(&self, ctx: SimulationContext) -> Network {
        match self {
            NetworkConfig::ConstantBandwidth { bandwidth, latency } => {
                Network::new(
                    Box::new(ConstantBandwidthNetworkModel::new(
                        *bandwidth,     // keep MB/s since data item sizes are in MB
                        latency * 1e-6, // convert to seconds
                    )),
                    ctx,
                )
            }
            NetworkConfig::SharedBandwidth { bandwidth, latency } => {
                Network::new(
                    Box::new(SharedBandwidthNetworkModel::new(
                        *bandwidth,     // keep MB/s since data item sizes are in MB
                        latency * 1e-6, // convert to seconds
                    )),
                    ctx,
                )
            }
            NetworkConfig::TopologyAware { .. } => Network::new(Box::new(TopologyAwareNetworkModel::new()), ctx),
        }
    }

    /// Adds network nodes and links (in case of topology-aware network model).
    pub fn init_network(&self, network: Rc<RefCell<Network>>, runner_id: Id, resources: &[Resource]) {
        let mut network = network.borrow_mut();

        // Add nodes
        for (host_name, id) in resources
            .iter()
            .map(|r| (r.name.as_str(), r.id))
            .chain([("master", runner_id)])
        {
            network.add_node(
                host_name,
                // since local transfers are not used,
                // we don't care about the local model parameters
                Box::new(ConstantBandwidthNetworkModel::new(100000., 0.)),
            );
            network.set_location(id, host_name);
        }

        // Add links
        if let NetworkConfig::TopologyAware {
            topology_type,
            link_bandwidth,
            link_latency,
        } = self
        {
            let link_latency = link_latency * 1e-6; // convert to seconds

            match topology_type {
                TopologyType::Star => {
                    for resource in resources.iter() {
                        network.add_full_duplex_link(
                            "master",
                            &resource.name,
                            Link::shared(*link_bandwidth, link_latency),
                        );
                    }
                }
                TopologyType::FullMesh => {
                    for host1 in resources.iter().map(|r| r.name.as_str()).chain(["master"]) {
                        for host2 in resources.iter().map(|r| r.name.as_str()).chain(["master"]) {
                            if host1 < host2 {
                                network.add_full_duplex_link(host1, host2, Link::shared(*link_bandwidth, link_latency));
                            }
                        }
                    }
                }
            }

            network.init_topology();
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Yaml {
    network: NetworkConfig,
}

/// Reads network model configuration from YAML file.
///
/// Configuration file example:
/// <https://github.com/osukhoroslov/dslab/blob/main/examples/dag-demo/systems/cluster-het-4-32cores.yaml>
pub fn read_network_config<P: AsRef<Path>>(file: P) -> NetworkConfig {
    let network: Yaml = serde_yaml::from_str(
        &std::fs::read_to_string(&file).unwrap_or_else(|_| panic!("Can't read file {}", file.as_ref().display())),
    )
    .unwrap_or_else(|e| panic!("Can't parse YAML from file {}: {e:?}", file.as_ref().display()));

    network.network
}
