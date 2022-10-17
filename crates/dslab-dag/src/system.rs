//! System.

use dslab_core::component::Id;
use dslab_network::network::Network;

use crate::data_item::DataTransferMode;
use crate::resource::Resource;

/// Describes a system as a set of resources and a network.
#[derive(Clone, Copy)]
pub struct System<'a> {
    pub resources: &'a Vec<Resource>,
    pub network: &'a Network,
}

impl System<'_> {
    /// Returns average time over all resources for executing one flop.
    pub fn avg_flop_time(&self) -> f64 {
        self.resources.iter().map(|r| 1. / r.speed as f64).sum::<f64>() / self.resources.len() as f64
    }

    /// Returns average time over all pairs of resources for sending one unit of data.
    pub fn avg_net_time(&self, id: Id, data_transfer_mode: &DataTransferMode) -> f64 {
        self.resources
            .iter()
            .map(|r1| {
                self.resources
                    .iter()
                    .map(|r2| data_transfer_mode.net_time(self.network, r1.id, r2.id, id))
                    .sum::<f64>()
            })
            .sum::<f64>()
            / (self.resources.len() as f64).powf(2.)
    }

    /// Returns average time over all resources for sending one unit of data to runner.
    pub fn avg_upload_net_time(&self, id: Id) -> f64 {
        self.resources
            .iter()
            .map(|r| 1. / self.network.bandwidth(r.id, id))
            .sum::<f64>()
            / self.resources.len() as f64
    }
}
