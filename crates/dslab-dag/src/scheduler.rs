//! DAG scheduling.

use dslab_core::component::Id;
use dslab_core::context::SimulationContext;
use dslab_network::network::Network;

use crate::dag::DAG;
use crate::resource::Resource;
use crate::runner::Config;
use crate::task::TaskState;

/// Represents an action ordered by the scheduler.
#[derive(Debug)]
pub enum Action {
    /// Execute the task on the resource using a given *number* of cores.
    ScheduleTask { task: usize, resource: usize, cores: u32 },
    /// Execute the task on the resource using a given *set* of cores.
    ScheduleTaskOnCores {
        task: usize,
        resource: usize,
        cores: Vec<u32>,
    },
    /// Transfer data item between the specified resources.
    /// Action will be queued if there is no such data item right now.
    TransferData { data_item: usize, from: Id, to: Id },
}

/// Trait for implementing DAG scheduling algorithms.
///
/// Includes callback methods which can return one or multiple actions corresponding to decisions
/// made by the scheduler (assign task to resource, transfer data item between resources, etc).
pub trait Scheduler {
    /// Called once in the beginning of DAG execution.
    fn start(
        &mut self,
        dag: &DAG,
        resources: &Vec<Resource>,
        network: &Network,
        config: Config,
        ctx: &SimulationContext,
    ) -> Vec<Action>;
    /// Called on every task state change.
    ///
    /// Useful for implementing dynamic scheduling algorithms.
    /// For static algorithms just return `Vec::new()`.
    fn on_task_state_changed(
        &mut self,
        task: usize,
        task_state: TaskState,
        dag: &DAG,
        resources: &Vec<Resource>,
        ctx: &SimulationContext,
    ) -> Vec<Action>;
}
