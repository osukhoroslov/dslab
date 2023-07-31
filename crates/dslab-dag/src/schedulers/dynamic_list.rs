use std::collections::HashMap;
use std::str::FromStr;
use std::string::ToString;

use strum_macros::{Display, EnumIter, EnumString};

use dslab_core::context::SimulationContext;

use crate::dag::DAG;
use crate::data_item::DataTransferMode;
use crate::runner::Config;
use crate::scheduler::{Action, Scheduler, SchedulerParams};
use crate::schedulers::common::calc_ranks;
use crate::system::System;
use crate::task::*;

struct Resource {
    cores_available: u32,
    memory_available: u64,
    speed: f64,
}

#[derive(Clone, Debug, Display, EnumIter, EnumString)]
pub enum TaskCriterion {
    CompSize,
    DataSize,
    ChildrenCount,
    BottomLevel,
}

#[derive(Clone, Debug, PartialEq, Display, EnumIter, EnumString)]
pub enum ResourceCriterion {
    Speed,
    TaskData,
    IdleCores,
}

#[derive(Clone, Debug, Display, EnumIter, EnumString)]
pub enum CoresCriterion {
    MaxCores,
    Efficiency90,
    Efficiency50,
}

#[derive(Clone, Debug)]
pub struct DynamicListStrategy {
    pub task_criterion: TaskCriterion,
    pub resource_criterion: ResourceCriterion,
    pub cores_criterion: CoresCriterion,
}

impl DynamicListStrategy {
    pub fn from_params(params: &SchedulerParams) -> Self {
        let task_criterion_str: String = params.get("task").unwrap();
        let resource_criterion_str: String = params.get("resource").unwrap();
        let cores_criterion_str: String = params.get("cores").unwrap_or(CoresCriterion::MaxCores.to_string());
        Self {
            task_criterion: TaskCriterion::from_str(&task_criterion_str)
                .expect("Wrong task criterion: {task_criterion_str}"),
            resource_criterion: ResourceCriterion::from_str(&resource_criterion_str)
                .expect("Wrong resource criterion: {resource_criterion_str}"),
            cores_criterion: CoresCriterion::from_str(&cores_criterion_str)
                .expect("Wrong cores criterion: {cores_criterion_str}"),
        }
    }
}

pub struct DynamicListScheduler {
    pub strategy: DynamicListStrategy,
    data_location: HashMap<usize, usize>,
}

impl DynamicListScheduler {
    pub fn new(strategy: DynamicListStrategy) -> Self {
        DynamicListScheduler {
            strategy,
            data_location: HashMap::new(),
        }
    }

    pub fn from_params(params: &SchedulerParams) -> Self {
        Self::new(DynamicListStrategy::from_params(params))
    }

    fn schedule(&mut self, dag: &DAG, system: System, ctx: &SimulationContext) -> Vec<Action> {
        let mut resources: Vec<Resource> = system
            .resources
            .iter()
            .map(|resource| Resource {
                cores_available: resource.cores_available,
                memory_available: resource.memory_available,
                speed: resource.speed,
            })
            .collect();
        let mut result: Vec<Action> = Vec::new();

        let avg_net_time = system.avg_net_time(ctx.id(), &DataTransferMode::Direct);
        let rank = calc_ranks(system.avg_flop_time(), avg_net_time, dag);

        let get_data_size = |task_id: usize| -> f64 {
            let task = dag.get_task(task_id);

            let mut data_items = task.inputs.clone();
            data_items.extend(task.outputs.clone());
            data_items
                .into_iter()
                .map(|data_item| dag.get_data_item(data_item).size)
                .sum::<f64>()
        };

        let mut ready_tasks = dag.get_ready_tasks().iter().cloned().collect::<Vec<usize>>();
        ready_tasks.sort_by(|&a, &b| match self.strategy.task_criterion {
            TaskCriterion::CompSize => dag.get_task(b).flops.total_cmp(&dag.get_task(a).flops),
            TaskCriterion::DataSize => get_data_size(b).total_cmp(&get_data_size(a)),
            TaskCriterion::ChildrenCount => dag.get_task(b).outputs.len().cmp(&dag.get_task(a).outputs.len()),
            TaskCriterion::BottomLevel => rank[b].total_cmp(&rank[a]),
        });

        for task in ready_tasks.into_iter() {
            let mut total_task_data: HashMap<usize, f64> = HashMap::new();
            if self.strategy.resource_criterion == ResourceCriterion::TaskData {
                for input in dag.get_task(task).inputs.iter() {
                    if let Some(location) = self.data_location.get(input) {
                        *total_task_data.entry(*location).or_default() += dag.get_data_item(*input).size;
                    }
                }
            }

            let best_resource = (0..resources.len())
                .filter(|&r| dag.get_task(task).is_allowed_on(r))
                .filter(|&r| {
                    resources[r].cores_available >= dag.get_task(task).min_cores
                        && resources[r].memory_available >= dag.get_task(task).memory
                })
                .min_by(|&a, &b| match self.strategy.resource_criterion {
                    ResourceCriterion::Speed => resources[b].speed.total_cmp(&resources[a].speed),
                    ResourceCriterion::TaskData => total_task_data
                        .get(&b)
                        .unwrap_or(&0.)
                        .total_cmp(total_task_data.get(&a).unwrap_or(&0.)),
                    ResourceCriterion::IdleCores => resources[b].cores_available.cmp(&resources[a].cores_available),
                });

            if best_resource.is_none() {
                break;
            }
            let best_resource = best_resource.unwrap();

            let get_max_cores_for_efficiency = |efficiency: f64| -> u32 {
                for cores in (1..resources[best_resource].cores_available).rev() {
                    let cur = dag.get_task(task).cores_dependency.speedup(cores) / cores as f64;
                    if cur >= efficiency {
                        return cores;
                    }
                }
                1
            };

            let cores = match self.strategy.cores_criterion {
                CoresCriterion::MaxCores => resources[best_resource].cores_available,
                CoresCriterion::Efficiency90 => get_max_cores_for_efficiency(0.9),
                CoresCriterion::Efficiency50 => get_max_cores_for_efficiency(0.5),
            };
            let cores = cores.clamp(dag.get_task(task).min_cores, dag.get_task(task).max_cores);

            resources[best_resource].cores_available -= cores;
            resources[best_resource].memory_available -= dag.get_task(task).memory;
            result.push(Action::ScheduleTask {
                task,
                resource: best_resource,
                cores,
                expected_span: None,
            });
            for &data_item in dag.get_task(task).outputs.iter() {
                self.data_location.insert(data_item, best_resource);
            }
        }
        result
    }
}

impl Scheduler for DynamicListScheduler {
    fn start(&mut self, dag: &DAG, system: System, config: Config, ctx: &SimulationContext) -> Vec<Action> {
        assert_ne!(
            config.data_transfer_mode,
            DataTransferMode::Manual,
            "DynamicListScheduler doesn't support DataTransferMode::Manual"
        );
        self.schedule(dag, system, ctx)
    }

    fn on_task_state_changed(
        &mut self,
        _task: usize,
        _task_state: TaskState,
        dag: &DAG,
        system: System,
        ctx: &SimulationContext,
    ) -> Vec<Action> {
        self.schedule(dag, system, ctx)
    }

    fn is_static(&self) -> bool {
        false
    }
}
