//! Network model tools.

use std::cell::RefCell;
use std::path::Path;
use std::rc::Rc;

use serde::{Deserialize, Serialize};

use dslab_network::constant_bandwidth_model::ConstantBandwidthNetwork;
use dslab_network::model::NetworkModel;
use dslab_network::shared_bandwidth_model::SharedBandwidthNetwork;

/// Represents network model parameters.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NetworkConfig {
    model: String,
    /// Network bandwidth in MB/s.
    bandwidth: f64,
    /// Network latency in μs.
    latency: f64,
}

impl NetworkConfig {
    /// Creates network config with ConstantBandwidthNetwork model.
    ///
    /// Bandwidth should be in MB/s, latency in μs.
    pub fn constant(bandwidth: f64, latency: f64) -> Self {
        Self {
            model: "ConstantBandwidthNetwork".to_string(),
            bandwidth,
            latency,
        }
    }

    /// Creates network config with SharedBandwidthNetwork model
    ///
    /// Bandwidth should be in MB/s, latency in μs.
    pub fn shared(bandwidth: f64, latency: f64) -> Self {
        Self {
            model: "SharedBandwidthNetwork".to_string(),
            bandwidth,
            latency,
        }
    }
}

impl NetworkConfig {
    /// Creates network model based on stored parameters.
    pub fn make_network(&self) -> Option<Rc<RefCell<dyn NetworkModel>>> {
        if self.model == "ConstantBandwidthNetwork" {
            Some(Rc::new(RefCell::new(ConstantBandwidthNetwork::new(
                self.bandwidth,      // keep MB/s since data item sizes are in MB
                self.latency * 1e-6, // convert to seconds
            ))))
        } else if self.model == "SharedBandwidthNetwork" {
            Some(Rc::new(RefCell::new(SharedBandwidthNetwork::new(
                self.bandwidth,      // keep MB/s since data item sizes are in MB
                self.latency * 1e-6, // convert to seconds
            ))))
        } else {
            None
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
    .unwrap_or_else(|_| panic!("Can't parse YAML from file {}", file.as_ref().display()));

    network.network
}
