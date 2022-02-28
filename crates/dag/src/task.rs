use compute::multicore::CoresDependency;

#[derive(Eq, PartialEq, Copy, Clone, Debug)]
pub enum TaskState {
    Pending,
    Ready,
    Scheduled,
    Runnable,
    Running,
    Done,
}

#[derive(Clone, Debug)]
pub struct Task {
    pub name: String,
    pub flops: u64,
    pub memory: u64,
    pub min_cores: u32,
    pub max_cores: u32,
    pub cores_dependency: CoresDependency,
    pub state: TaskState,
    pub inputs: Vec<usize>,
    pub(crate) outputs: Vec<usize>,
    pub(crate) ready_inputs: usize,
}

impl Task {
    pub fn new(
        name: &str,
        flops: u64,
        memory: u64,
        min_cores: u32,
        max_cores: u32,
        cores_dependency: CoresDependency,
    ) -> Self {
        Self {
            name: name.to_string(),
            flops,
            memory,
            min_cores,
            max_cores,
            cores_dependency,
            state: TaskState::Ready,
            inputs: Vec::new(),
            outputs: Vec::new(),
            ready_inputs: 0,
        }
    }

    pub fn add_input(&mut self, data_item_id: usize) {
        self.inputs.push(data_item_id);
    }

    pub fn add_output(&mut self, data_item_id: usize) {
        self.outputs.push(data_item_id);
    }
}
