//! Data item.

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
    pub(crate) consumers: Vec<usize>,
    pub(crate) is_input: bool,
    pub(crate) state: DataItemState,
}

impl DataItem {
    /// Creates new data item.
    pub fn new(name: &str, size: u64, state: DataItemState, is_input: bool) -> Self {
        Self {
            name: name.to_string(),
            size,
            consumers: Vec::new(),
            is_input,
            state,
        }
    }

    /// Adds a [task](crate::task::Task) that consumes the data item.
    pub fn add_consumer(&mut self, consumer: usize) {
        self.consumers.push(consumer);
    }
}
