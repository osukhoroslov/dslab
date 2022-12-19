//! Data item.

use dslab_core::component::Id;
use dslab_network::network::Network;

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
    pub size: u64,
    pub producer: Option<usize>,
    pub(crate) consumers: Vec<usize>,
    pub(crate) state: DataItemState,
}

impl DataItem {
    /// Creates new data item.
    pub fn new(name: &str, size: u64, state: DataItemState, producer: Option<usize>) -> Self {
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
#[derive(Clone, PartialEq, Debug)]
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
    /// Calculates the data transfer time per data unit between the specified resources (src, dest).
    pub fn net_time(&self, network: &Network, src: Id, dst: Id, runner: Id) -> f64 {
        match self {
            DataTransferMode::ViaMasterNode => {
                1. / network.bandwidth(src, runner) + 1. / network.bandwidth(runner, dst)
            }
            DataTransferMode::Direct => 1. / network.bandwidth(src, dst),
            DataTransferMode::Manual => 0.,
        }
    }
}

pub enum DataTransferStrategy {
    Eager, // default assumption in HEFT -- data transfer starts as soon as task finished
    Lazy,  // data transfer starts only when the destination node is ready to execute the task
}
