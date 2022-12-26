//! Network model tools.

use std::cell::RefCell;
use std::rc::Rc;

use serde::{Deserialize, Serialize};

use dslab_network::constant_bandwidth_model::ConstantBandwidthNetwork;
use dslab_network::model::NetworkModel;
use dslab_network::shared_bandwidth_model::SharedBandwidthNetwork;

/// Represents network model parameters.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Network {
    model: String,
    bandwidth: f64,
    latency: f64,
}

impl Network {
    pub fn make_network(&self) -> Option<Rc<RefCell<dyn NetworkModel>>> {
        if self.model == "ConstantBandwidthNetwork" {
            Some(Rc::new(RefCell::new(ConstantBandwidthNetwork::new(
                self.bandwidth,
                self.latency,
            ))))
        } else if self.model == "SharedBandwidthNetwork" {
            Some(Rc::new(RefCell::new(SharedBandwidthNetwork::new(
                self.bandwidth,
                self.latency,
            ))))
        } else {
            None
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Yaml {
    network: Network,
}

/// Reads network model configuration from YAML file and creates the corresponding network model.
///
/// Configuration file example: https://github.com/osukhoroslov/dslab/blob/main/examples/dag/networks/network1.yaml
pub fn load_network(file: &str) -> Rc<RefCell<dyn NetworkModel>> {
    let network = read_network(file);

    match network.make_network() {
        Some(x) => x,
        None => {
            eprintln!("Unknown network model {}", network.model);
            std::process::exit(1);
        }
    }
}

pub fn read_network(file: &str) -> Network {
    let network: Yaml =
        serde_yaml::from_str(&std::fs::read_to_string(file).expect(&format!("Can't read file {}", file)))
            .expect(&format!("Can't parse YAML from file {}", file));

    network.network
}
