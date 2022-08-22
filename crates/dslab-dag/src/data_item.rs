//! DAG data item.

#[derive(Eq, PartialEq, Clone)]
pub enum DataItemState {
    /// Not ready, waiting for corresponding task to complete.
    Pending,
    /// Ready to be used in dependent tasks.
    Ready,
}

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

    /// Adds a [task](crate::task::Task) as a consumer
    pub fn add_consumer(&mut self, consumer: usize) {
        self.consumers.push(consumer);
    }
}
