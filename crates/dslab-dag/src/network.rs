//! Network model tools.

use std::cell::RefCell;
use std::path::Path;
use std::rc::Rc;

use serde::{Deserialize, Serialize};

use dslab_core::context::SimulationContext;
use dslab_core::Id;
use dslab_network::constant_bandwidth_model::ConstantBandwidthNetwork;
use dslab_network::network::Network;
use dslab_network::shared_bandwidth_model::SharedBandwidthNetwork;
use dslab_network::topology::Topology;
use dslab_network::topology_model::TopologyNetwork;

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
    ConstantBandwidthNetwork {
        /// Network bandwidth in MB/s.
        bandwidth: f64,
        /// Network latency in μs.
        latency: f64,
    },
    SharedBandwidthNetwork {
        /// Network bandwidth in MB/s.
        bandwidth: f64,
        /// Network latency in μs.
        latency: f64,
    },
    TopologyNetwork {
        #[serde(rename = "topology")]
        topology_type: TopologyType,
        /// Local node bandwidth in MB/s.
        local_bandwidth: f64,
        /// Local latency in μs.
        local_latency: f64,
        /// Links bandwidth in MB/s.
        link_bandwidth: f64,
        /// Links latency in μs.
        link_latency: f64,
    },
}

impl NetworkConfig {
    /// Creates network config with ConstantBandwidthNetwork model.
    ///
    /// Bandwidth should be in MB/s, latency in μs.
    pub fn constant(bandwidth: f64, latency: f64) -> Self {
        NetworkConfig::ConstantBandwidthNetwork { bandwidth, latency }
    }

    /// Creates network config with SharedBandwidthNetwork model
    ///
    /// Bandwidth should be in MB/s, latency in μs.
    pub fn shared(bandwidth: f64, latency: f64) -> Self {
        NetworkConfig::SharedBandwidthNetwork { bandwidth, latency }
    }
}

impl NetworkConfig {
    /// Creates network model based on stored parameters.
    pub fn make_network(&self, ctx: SimulationContext) -> Network {
        match self {
            NetworkConfig::ConstantBandwidthNetwork { bandwidth, latency } => {
                Network::new(
                    Rc::new(RefCell::new(ConstantBandwidthNetwork::new(
                        *bandwidth,     // keep MB/s since data item sizes are in MB
                        latency * 1e-6, // convert to seconds
                    ))),
                    ctx,
                )
            }
            NetworkConfig::SharedBandwidthNetwork { bandwidth, latency } => {
                Network::new(
                    Rc::new(RefCell::new(SharedBandwidthNetwork::new(
                        *bandwidth,     // keep MB/s since data item sizes are in MB
                        latency * 1e-6, // convert to seconds
                    ))),
                    ctx,
                )
            }
            NetworkConfig::TopologyNetwork { .. } => {
                let topology = Rc::new(RefCell::new(Topology::new()));
                Network::new_with_topology(
                    Rc::new(RefCell::new(TopologyNetwork::new(topology.clone()))),
                    topology,
                    ctx,
                )
            }
        }
    }

    pub fn init_network(&self, network: Rc<RefCell<Network>>, runner_id: Id, resource_ids: &[Id]) {
        if let NetworkConfig::TopologyNetwork {
            topology_type,
            local_bandwidth,
            local_latency,
            link_bandwidth,
            link_latency,
        } = self
        {
            let local_latency = local_latency * 1e-6;
            let link_latency = link_latency * 1e-6;
            let mut network = network.borrow_mut();

            let host_name = |id: Id| -> String { format!("host_{}", id) };

            for id in resource_ids.iter().copied().chain([runner_id]) {
                network.add_node(
                    &host_name(id),
                    Box::new(SharedBandwidthNetwork::new(*local_bandwidth, local_latency)),
                );
                network.set_location(id, &host_name(id));
            }

            match topology_type {
                TopologyType::Star => {
                    for id in resource_ids.iter().copied() {
                        network.add_link(&host_name(runner_id), &host_name(id), *link_bandwidth, link_latency);
                    }
                }
                TopologyType::FullMesh => {
                    for id1 in resource_ids.iter().copied().chain([runner_id]) {
                        for id2 in resource_ids.iter().copied().chain([runner_id]) {
                            if id1 < id2 {
                                network.add_link(&host_name(id1), &host_name(id2), *link_bandwidth, link_latency);
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
/// https://github.com/osukhoroslov/dslab/blob/main/examples/dag-demo/systems/cluster-het-4-32cores.yaml
pub fn read_network_config<P: AsRef<Path>>(file: P) -> NetworkConfig {
    let network: Yaml = serde_yaml::from_str(
        &std::fs::read_to_string(&file).unwrap_or_else(|_| panic!("Can't read file {}", file.as_ref().display())),
    )
    .unwrap_or_else(|e| panic!("Can't parse YAML from file {}: {e:?}", file.as_ref().display()));

    network.network
}
