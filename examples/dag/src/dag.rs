use compute::multicore::CoresDependency;
use std::collections::BTreeSet;

// TASK ////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Eq, PartialEq, Clone)]
pub enum TaskState {
    Pending,
    Ready,
    Scheduled,
    Done,
}

#[derive(Clone)]
pub struct Task {
    pub name: String,
    pub flops: u64,
    pub memory: u64,
    pub min_cores: u32,
    pub max_cores: u32,
    pub cores_dependency: CoresDependency,
    pub state: TaskState,
    pub inputs: Vec<usize>,
    outputs: Vec<usize>,
    ready_inputs: usize,
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

// DATA ITEM ///////////////////////////////////////////////////////////////////////////////////////

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
    state: DataItemState,
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

// DAG /////////////////////////////////////////////////////////////////////////////////////////////

pub struct DAG {
    tasks: Vec<Task>,
    data_items: Vec<DataItem>,
    ready_tasks: BTreeSet<usize>,
    completed_task_count: usize,
}

impl DAG {
    pub fn new() -> Self {
        Self {
            tasks: Vec::new(),
            data_items: Vec::new(),
            ready_tasks: BTreeSet::new(),
            completed_task_count: 0,
        }
    }

    pub fn add_task(
        &mut self,
        name: &str,
        flops: u64,
        memory: u64,
        min_cores: u32,
        max_cores: u32,
        cores_dependency: CoresDependency,
    ) -> usize {
        let task = Task::new(name, flops, memory, min_cores, max_cores, cores_dependency);
        let task_id = self.tasks.len();
        self.tasks.push(task);
        self.ready_tasks.insert(task_id);
        task_id
    }

    pub fn get_task(&self, task_id: usize) -> &Task {
        self.tasks.get(task_id).unwrap()
    }

    pub fn get_data_item(&self, data_id: usize) -> &DataItem {
        self.data_items.get(data_id).unwrap()
    }

    pub fn get_ready_tasks(&self) -> &BTreeSet<usize> {
        &self.ready_tasks
    }

    pub fn add_data_item(&mut self, name: &str, size: u64) -> usize {
        let data_item = DataItem::new(name, size, DataItemState::Ready);
        let data_item_id = self.data_items.len();
        self.data_items.push(data_item);
        data_item_id
    }

    pub fn add_task_output(&mut self, producer: usize, name: &str, size: u64) -> usize {
        let data_item = DataItem::new(name, size, DataItemState::Pending);
        let data_item_id = self.data_items.len();
        self.data_items.push(data_item);
        self.tasks.get_mut(producer).unwrap().add_output(data_item_id);
        data_item_id
    }

    pub fn add_data_dependency(&mut self, data_item_id: usize, consumer_id: usize) {
        let data_item = self.data_items.get_mut(data_item_id).unwrap();
        data_item.add_consumer(consumer_id);
        let consumer = self.tasks.get_mut(consumer_id).unwrap();
        consumer.add_input(data_item_id);
        if data_item.state == DataItemState::Pending && consumer.state == TaskState::Ready {
            consumer.state = TaskState::Pending;
            self.ready_tasks.remove(&consumer_id);
        }
    }

    pub fn update_task_state(&mut self, task_id: usize, state: TaskState) -> Vec<usize> {
        let mut task = self.tasks.get_mut(task_id).unwrap();
        task.state = state;
        match task.state {
            TaskState::Scheduled => {
                self.ready_tasks.remove(&task_id);
                Vec::new()
            }
            TaskState::Done => {
                self.completed_task_count += 1;
                task.outputs.clone()
            }
            _ => Vec::new(),
        }
    }

    pub fn update_data_item_state(&mut self, data_id: usize, state: DataItemState) {
        let mut data_item = self.data_items.get_mut(data_id).unwrap();
        data_item.state = state;
        match data_item.state {
            DataItemState::Ready => {
                for t in data_item.consumers.iter() {
                    let mut consumer = self.tasks.get_mut(*t).unwrap();
                    consumer.ready_inputs += 1;
                    if consumer.ready_inputs == consumer.inputs.len() {
                        consumer.state = TaskState::Ready;
                        self.ready_tasks.insert(*t);
                    }
                }
            }
            _ => {}
        }
    }

    pub fn is_completed(&self) -> bool {
        self.tasks.len() == self.completed_task_count
    }
}
