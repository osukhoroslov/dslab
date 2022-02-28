#[derive(Eq, PartialEq)]
pub enum DataItemState {
    Pending,
    Ready,
}

pub struct DataItem {
    pub name: String,
    pub size: u64,
    pub consumers: Vec<usize>,
    pub(crate) state: DataItemState,
}

impl DataItem {
    pub fn new(name: &str, size: u64, state: DataItemState) -> Self {
        Self {
            name: name.to_string(),
            size,
            consumers: Vec::new(),
            state,
        }
    }

    pub fn add_consumer(&mut self, consumer: usize) {
        self.consumers.push(consumer);
    }
}
