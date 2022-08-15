use std::cell::RefCell;
use std::rc::Rc;

use serde::{Deserialize, Serialize};

use dslab_network::constant_bandwidth_model::ConstantBandwidthNetwork;
use dslab_network::model::NetworkModel;
use dslab_network::shared_bandwidth_model::SharedBandwidthNetwork;

#[derive(Debug, Serialize, Deserialize)]
struct Network {
    model: String,
    bandwidth: f64,
    latency: f64,
}

#[derive(Debug, Serialize, Deserialize)]
struct Yaml {
    network: Network,
}

/// Reads network configuration from
/// [YAML file](https://github.com/osukhoroslov/dslab/blob/main/examples/dag/networks/network1.yaml).
pub fn load_network(file: &str) -> Rc<RefCell<dyn NetworkModel>> {
    let network: Yaml =
        serde_yaml::from_str(&std::fs::read_to_string(file).expect(&format!("Can't read file {}", file)))
            .expect(&format!("Can't parse YAML from file {}", file));

    let network = network.network;

    if network.model == "ConstantBandwidthNetwork" {
        Rc::new(RefCell::new(ConstantBandwidthNetwork::new(
            network.bandwidth,
            network.latency,
        )))
    } else if network.model == "SharedBandwidthNetwork" {
        Rc::new(RefCell::new(SharedBandwidthNetwork::new(
            network.bandwidth,
            network.latency,
        )))
    } else {
        eprintln!("Unknown network model {}", network.model);
        std::process::exit(1);
    }
}
