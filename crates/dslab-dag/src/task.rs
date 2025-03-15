//! DAG task.

use std::collections::BTreeSet;

use strum_macros::EnumIter;

use dslab_compute::multicore::CoresDependency;

/// Represents a DAG task state.
#[derive(Eq, PartialEq, Copy, Clone, Debug, EnumIter)]
pub enum TaskState {
    /// Waiting for its dependencies.
    Pending,
    /// All dependencies are satisfied, ready to be scheduled.
    Ready,
    /// Task is scheduled, waiting for its dependencies.
    Scheduled,
    /// All dependencies are satisfied and task is scheduled.
    Runnable,
    /// Task is running.
    Running,
    /// Task is completed.
    Done,
}

/// Restricts task execution to some subset of resources.
#[derive(Clone, Debug)]
pub enum ResourceRestriction {
    /// Allows execution only on the given resources.
    Only(BTreeSet<usize>),
    /// Allows execution on any resource except the given resources.
    Except(BTreeSet<usize>),
}

impl ResourceRestriction {
    pub fn is_allowed_on(&self, resource_id: usize) -> bool {
        match self {
            ResourceRestriction::Only(set) => set.contains(&resource_id),
            ResourceRestriction::Except(set) => !set.contains(&resource_id),
        }
    }
}

/// Represents a DAG task.
///
/// Described by the amount of computations in flops, the minimum and maximum number of used cores, and the amount of
/// used memory. Also has a function which defines the dependence of parallel speedup on the number of used cores.
///
/// Each task can consume (as task inputs) and produce (as task inputs) one or more data items.
#[derive(Clone, Debug)]
pub struct Task {
    pub name: String,
    /// The amount of computations performed by this task in Gflops.
    pub flops: f64,
    /// Memory demand of this task in MB.
    pub memory: u64,
    pub min_cores: u32,
    pub max_cores: u32,
    pub cores_dependency: CoresDependency,
    pub state: TaskState,
    pub inputs: Vec<usize>,
    pub outputs: Vec<usize>,
    pub(crate) ready_inputs: usize,
    pub resource_restrictions: Vec<ResourceRestriction>,
}

impl Task {
    /// Creates new task.
    pub fn new(
        name: &str,
        flops: f64,
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
            resource_restrictions: Vec::new(),
        }
    }

    /// Adds task input.
    pub fn add_input(&mut self, data_item_id: usize) {
        self.inputs.push(data_item_id);
    }

    /// Adds task output.
    pub fn add_output(&mut self, data_item_id: usize) {
        self.outputs.push(data_item_id);
    }

    /// Adds resource restriction.
    pub fn add_resource_restriction(&mut self, restriction: ResourceRestriction) {
        self.resource_restrictions.push(restriction);
    }

    pub fn is_allowed_on(&self, resource_id: usize) -> bool {
        self.resource_restrictions
            .iter()
            .all(|rr| rr.is_allowed_on(resource_id))
    }
}
