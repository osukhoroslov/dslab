//! DAG model of computation.

use std::collections::BTreeSet;
use std::path::Path;

use dslab_compute::multicore::CoresDependency;

use crate::dag_stats::DagStats;
use crate::data_item::*;
use crate::parsers::config::ParserConfig;
use crate::task::*;

/// Represents a computation consisting of multiple tasks with data dependencies
/// modeled as a directed acyclic graph (DAG).
///
/// Each task can produce one or more data items (task outputs) and consume (as task inputs) data items produced by
/// other tasks. Entry tasks consume separate data items corresponding to the DAG inputs. The data dependencies between
/// the tasks define constraints on task execution - a task cannot start its execution on some resource until all its
/// inputs are produced (parent tasks are completed) and transferred to this resource.
#[derive(Clone, Default)]
pub struct DAG {
    tasks: Vec<Task>,
    data_items: Vec<DataItem>,
    ready_tasks: BTreeSet<usize>,
    completed_task_count: usize,
    inputs: BTreeSet<usize>,
    outputs: BTreeSet<usize>,
}

impl DAG {
    /// Creates empty DAG.
    pub fn new() -> Self {
        Self::default()
    }

    /// Reads DAG from file in one of supported formats:
    /// - YAML format (.yaml extension)
    /// - WfCommons format (.json extension)
    /// - DAX format (.xml extension)
    /// - DOT format (.dot extension)
    pub fn from_file<P: AsRef<Path>>(file: P, parser_config: &ParserConfig) -> Self {
        match file.as_ref().extension().unwrap().to_str().unwrap() {
            "yaml" => DAG::from_yaml(file, parser_config),
            "json" => DAG::from_wfcommons(file, parser_config),
            "xml" => DAG::from_dax(file, parser_config),
            "dot" => DAG::from_dot(file),
            _ => {
                panic!("Unknown extension for dag: {}", file.as_ref().display());
            }
        }
    }

    /// Adds new [task](crate::task::Task) with provided parameters and returns its id.
    pub fn add_task(
        &mut self,
        name: &str,
        flops: f64,
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

    /// Returns task by id.
    pub fn get_task(&self, task_id: usize) -> &Task {
        self.tasks.get(task_id).unwrap()
    }

    /// Returns mutable task reference by id.
    pub fn get_task_mut(&mut self, task_id: usize) -> &mut Task {
        &mut self.tasks[task_id]
    }

    /// Returns all tasks.
    pub fn get_tasks(&self) -> &Vec<Task> {
        &self.tasks
    }

    /// Returns data item by id.
    pub fn get_data_item(&self, data_id: usize) -> &DataItem {
        self.data_items.get(data_id).unwrap()
    }

    /// Returns all data items.
    pub fn get_data_items(&self) -> &Vec<DataItem> {
        &self.data_items
    }

    /// Returns ids of [ready](crate::task::TaskState::Ready) tasks.
    pub fn get_ready_tasks(&self) -> &BTreeSet<usize> {
        &self.ready_tasks
    }

    pub fn get_inputs(&self) -> &BTreeSet<usize> {
        &self.inputs
    }

    pub fn get_outputs(&self) -> &BTreeSet<usize> {
        &self.outputs
    }

    /// Adds [data item](crate::data_item::DataItem) with provided parameters and returns its id.
    pub fn add_data_item(&mut self, name: &str, size: f64) -> usize {
        let data_item = DataItem::new(name, size, DataItemState::Ready, None);
        let data_item_id = self.data_items.len();
        self.data_items.push(data_item);
        self.inputs.insert(data_item_id);
        self.outputs.insert(data_item_id);
        data_item_id
    }

    /// Adds [data item](crate::data_item::DataItem) as a [task](crate::task::Task) output and returns its id.
    pub fn add_task_output(&mut self, producer: usize, name: &str, size: f64) -> usize {
        let data_item = DataItem::new(name, size, DataItemState::Pending, Some(producer));
        let data_item_id = self.data_items.len();
        self.data_items.push(data_item);
        self.tasks.get_mut(producer).unwrap().add_output(data_item_id);
        self.outputs.insert(data_item_id);
        data_item_id
    }

    /// Adds a dependency between [data item](crate::data_item::DataItem) and [task](crate::task::Task).
    pub fn add_data_dependency(&mut self, data_item_id: usize, consumer_id: usize) {
        let data_item = self.data_items.get_mut(data_item_id).unwrap();
        data_item.add_consumer(consumer_id);
        let consumer = self.tasks.get_mut(consumer_id).unwrap();
        consumer.add_input(data_item_id);
        self.outputs.remove(&data_item_id);
        if data_item.state == DataItemState::Pending && consumer.state == TaskState::Ready {
            consumer.state = TaskState::Pending;
            self.ready_tasks.remove(&consumer_id);
        } else if data_item.state == DataItemState::Ready {
            consumer.ready_inputs += 1;
        }
    }

    /// Updates task state to a provided value, updating dependent data item states if needed.
    pub fn update_task_state(&mut self, task_id: usize, state: TaskState) {
        let task = self.tasks.get_mut(task_id).unwrap();
        task.state = state;
        if task.state != TaskState::Ready {
            self.ready_tasks.remove(&task_id);
        }
        if task.state == TaskState::Done {
            self.completed_task_count += 1;
            for &data_item in task.outputs.clone().iter() {
                self.update_data_item_state(data_item, DataItemState::Ready);
            }
        }
    }

    /// Updates data item state to a provided value, updating dependent task states if needed.
    pub fn update_data_item_state(&mut self, data_id: usize, state: DataItemState) {
        let data_item = self.data_items.get_mut(data_id).unwrap();
        data_item.state = state;
        if data_item.state == DataItemState::Ready {
            for t in data_item.consumers.iter() {
                let consumer = self.tasks.get_mut(*t).unwrap();
                consumer.ready_inputs += 1;
                if consumer.ready_inputs == consumer.inputs.len() {
                    if consumer.state == TaskState::Pending {
                        consumer.state = TaskState::Ready;
                        self.ready_tasks.insert(*t);
                    } else if consumer.state == TaskState::Scheduled {
                        consumer.state = TaskState::Runnable;
                    } else {
                        panic!(
                            "Error: task {} reached needed number of ready inputs in state {:?}",
                            consumer.name, consumer.state
                        );
                    }
                }
            }
        }
    }

    /// Checks whether all tasks are completed.
    pub fn is_completed(&self) -> bool {
        self.tasks.len() == self.completed_task_count
    }

    pub fn stats(&self) -> DagStats {
        DagStats::new(self)
    }

    pub fn add_resource_restriction(&mut self, task_id: usize, restriction: ResourceRestriction) {
        self.tasks[task_id].add_resource_restriction(restriction);
    }

    /// Sets data item as output of the specified task.
    ///
    /// The data item must not have producer, i.e. it must be among the DAG inputs.
    pub(crate) fn set_as_task_output(&mut self, data_item_id: usize, producer: usize) {
        assert!(self.data_items[data_item_id].producer.is_none());
        assert!(self.inputs.remove(&data_item_id));
        self.tasks.get_mut(producer).unwrap().add_output(data_item_id);
        self.data_items[data_item_id].state = DataItemState::Pending;
        self.data_items[data_item_id].producer = Some(producer);
        let consumers = &self.data_items[data_item_id].consumers;
        for &consumer in consumers.iter() {
            self.tasks[consumer].state = TaskState::Pending;
            self.tasks[consumer].ready_inputs -= 1;
            self.ready_tasks.remove(&consumer);
        }
    }

    pub(crate) fn set_inputs(&mut self, inputs: BTreeSet<usize>) {
        assert!(self.inputs.is_empty());
        self.inputs = inputs
    }

    pub(crate) fn set_outputs(&mut self, outputs: BTreeSet<usize>) {
        assert!(self.outputs.is_empty());
        self.outputs = outputs
    }
}
