#[derive(Eq, PartialEq, Clone)]
pub enum DataItemState {
    Pending,
    Ready,
}

#[derive(Clone)]
pub struct DataItem {
    pub name: String,
    pub size: u64,
    pub consumers: Vec<usize>,
    pub is_input: bool,
    pub(crate) state: DataItemState,
}

impl DataItem {
    pub fn new(name: &str, size: u64, state: DataItemState, is_input: bool) -> Self {
        Self {
            name: name.to_string(),
            size,
            consumers: Vec::new(),
            is_input,
            state,
        }
    }

    pub fn add_consumer(&mut self, consumer: usize) {
        self.consumers.push(consumer);
    }
}
