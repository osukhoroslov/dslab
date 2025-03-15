//! Data item.

use std::str::FromStr;

use serde::Deserialize;

use dslab_network::Network;
use simcore::component::Id;

/// Represents a data item state.
#[derive(Eq, PartialEq, Clone)]
pub enum DataItemState {
    /// Not ready, the task producing the data item is not completed.
    Pending,
    /// The data item is produced and ready to be consumed by the dependent tasks.
    Ready,
}

/// Represents a data item produced or consumed by DAG tasks.
///
/// Data items are produced by DAG tasks or defined as DAG inputs.
#[derive(Clone)]
pub struct DataItem {
    pub name: String,
    /// The size of data item in MB.
    pub size: f64,
    pub producer: Option<usize>,
    pub consumers: Vec<usize>,
    pub(crate) state: DataItemState,
}

impl DataItem {
    /// Creates new data item.
    pub fn new(name: &str, size: f64, state: DataItemState, producer: Option<usize>) -> Self {
        Self {
            name: name.to_string(),
            size,
            producer,
            consumers: Vec::new(),
            state,
        }
    }

    /// Adds a [task](crate::task::Task) that consumes the data item.
    pub fn add_consumer(&mut self, consumer: usize) {
        self.consumers.push(consumer);
    }
}

/// Defines how data items are transferred during the DAG execution.
#[derive(Copy, Clone, PartialEq, Debug, Deserialize)]
pub enum DataTransferMode {
    /// Every data item is automatically transferred between producer and consumer
    /// via the master node (producer -> master -> consumer).
    ViaMasterNode,
    /// Every data item is automatically transferred between producer and consumer
    /// directly (producer -> consumer)
    Direct,
    /// Data items are not transferred automatically,
    /// all data transfers must be explicitly ordered by the scheduler.
    Manual,
}

impl DataTransferMode {
    /// Calculates the data transfer time per data unit between the specified resources (src, dst).
    pub fn net_time(&self, network: &Network, src: Id, dst: Id, runner: Id) -> f64 {
        match self {
            DataTransferMode::ViaMasterNode => {
                1. / network.bandwidth(src, runner) + 1. / network.bandwidth(runner, dst)
            }
            DataTransferMode::Direct => {
                if src == dst {
                    0.
                } else {
                    1. / network.bandwidth(src, dst)
                }
            }
            DataTransferMode::Manual => 0.,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DataTransferStrategy {
    Eager, // default assumption in HEFT -- data transfer starts as soon as task finished
    Lazy,  // data transfer starts only when the destination node is ready to execute the task
}

impl FromStr for DataTransferStrategy {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Eager" => Ok(DataTransferStrategy::Eager),
            "Lazy" => Ok(DataTransferStrategy::Lazy),
            _ => Err(()),
        }
    }
}
