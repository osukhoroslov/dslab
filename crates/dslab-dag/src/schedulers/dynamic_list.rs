use std::collections::HashMap;
use std::str::FromStr;
use std::string::ToString;

use itertools::Itertools;
use strum_macros::{Display, EnumIter, EnumString};

use simcore::context::SimulationContext;

use crate::dag::DAG;
use crate::data_item::DataTransferMode;
use crate::runner::Config;
use crate::scheduler::{Action, Scheduler, SchedulerParams};
use crate::schedulers::common::calc_ranks;
use crate::system::System;
use crate::task::*;

struct Resource {
    id: usize,
    cores: u32,
    cores_available: u32,
    memory: u64,
    memory_available: u64,
    speed: f64,
    norm_speed: f64,
}

#[derive(Clone, Debug, PartialEq, Display, EnumIter, EnumString)]
pub enum TaskCriterion {
    CompSize,
    DataSize,
    ChildrenCount,
    BottomLevel,
    Cores,
    Memory,
    CoresMemorySum,
    CoresMemoryMult,
    CoresFlops,
    MemoryFlops,
    CoresMemoryFlops,
    RankPack0,
    RankPack025,
    RankPack05,
    RankPack075,
    RankPack1,
    RankPackSel,
    RankPackMult,
}

#[derive(Clone, Debug, PartialEq, Display, EnumIter, EnumString)]
pub enum ResourceCriterion {
    Speed,
    TaskData,
    MaxAvailableCores,
    MaxAvailableMemory,
    MinAvailableCores,
    MinAvailableMemory,
    DotProduct,
    DotProductSpeed,
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

    /// Schedules tasks by iterating over tasks and selecting the best resource matching the task.
    fn schedule_by_task(&mut self, dag: &DAG, system: System, ctx: &SimulationContext) -> Vec<Action> {
        let max_speed = system
            .resources
            .iter()
            .max_by(|a, b| a.speed.total_cmp(&b.speed))
            .unwrap()
            .speed;
        let mut resources: Vec<Resource> = system
            .resources
            .iter()
            .enumerate()
            .map(|(id, resource)| Resource {
                id,
                cores: resource.cores,
                cores_available: resource.cores_available,
                memory: resource.memory,
                memory_available: resource.memory_available,
                speed: resource.speed,
                norm_speed: resource.speed / max_speed,
            })
            .collect();
        let mut result: Vec<Action> = Vec::new();

        let avg_net_time = system.avg_net_time(ctx.id(), &DataTransferMode::Direct);
        let task_ranks = calc_ranks(system.avg_flop_time(), avg_net_time, dag);
        let max_cores: u32 = dag
            .get_tasks()
            .iter()
            .map(|t| t.max_cores)
            .max_by(|a, b| a.cmp(b))
            .unwrap();
        let task_norm_cores: Vec<f64> = dag
            .get_tasks()
            .iter()
            .map(|t| t.max_cores as f64 / max_cores as f64)
            .collect();
        let max_memory: u64 = dag
            .get_tasks()
            .iter()
            .map(|t| t.memory)
            .max_by(|a, b| a.cmp(b))
            .unwrap();
        let task_norm_memory: Vec<f64> = dag
            .get_tasks()
            .iter()
            .map(|t| t.memory as f64 / max_memory as f64)
            .collect();
        let max_flops: f64 = dag
            .get_tasks()
            .iter()
            .map(|t| t.flops)
            .max_by(|a, b| a.total_cmp(b))
            .unwrap();
        let task_norm_flops: Vec<f64> = dag.get_tasks().iter().map(|t| t.flops / max_flops).collect();

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
            TaskCriterion::BottomLevel => task_ranks[b].total_cmp(&task_ranks[a]),
            TaskCriterion::Cores => dag
                .get_task(b)
                .max_cores
                .cmp(&dag.get_task(a).max_cores)
                .then(dag.get_task(b).flops.total_cmp(&dag.get_task(a).flops)),
            TaskCriterion::Memory => dag
                .get_task(b)
                .memory
                .cmp(&dag.get_task(a).memory)
                .then(dag.get_task(b).flops.total_cmp(&dag.get_task(a).flops)),
            TaskCriterion::CoresMemorySum => (task_norm_cores[b] + task_norm_memory[b])
                .total_cmp(&(task_norm_cores[a] + task_norm_memory[a]))
                .then(dag.get_task(b).flops.total_cmp(&dag.get_task(a).flops)),
            TaskCriterion::CoresMemoryMult => (task_norm_cores[b] * task_norm_memory[b])
                .total_cmp(&(task_norm_cores[a] * task_norm_memory[a]))
                .then(dag.get_task(b).flops.total_cmp(&dag.get_task(a).flops)),
            TaskCriterion::CoresFlops => (task_norm_cores[b] * task_norm_flops[b])
                .total_cmp(&(task_norm_cores[a] * task_norm_flops[a]))
                .then(dag.get_task(b).flops.total_cmp(&dag.get_task(a).flops)),
            TaskCriterion::MemoryFlops => (task_norm_memory[b] * task_norm_flops[b])
                .total_cmp(&(task_norm_memory[a] * task_norm_flops[a]))
                .then(dag.get_task(b).flops.total_cmp(&dag.get_task(a).flops)),
            TaskCriterion::CoresMemoryFlops => ((task_norm_cores[b] + task_norm_memory[b]) * task_norm_flops[b])
                .total_cmp(&((task_norm_cores[a] + task_norm_memory[b]) * task_norm_flops[a]))
                .then(dag.get_task(b).flops.total_cmp(&dag.get_task(a).flops)),
            _ => panic!("Should not happen"),
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

            let suitable_resources: Vec<usize> = (0..resources.len())
                .filter(|&r| dag.get_task(task).is_allowed_on(r))
                .filter(|&r| {
                    resources[r].cores_available >= dag.get_task(task).min_cores
                        && resources[r].memory_available >= dag.get_task(task).memory
                })
                .collect();
            if suitable_resources.is_empty() {
                continue;
            }

            let best_resource = suitable_resources
                .into_iter()
                .min_by(|&a, &b| match self.strategy.resource_criterion {
                    ResourceCriterion::Speed => resources[b].speed.total_cmp(&resources[a].speed),
                    ResourceCriterion::TaskData => total_task_data
                        .get(&b)
                        .unwrap_or(&0.)
                        .total_cmp(total_task_data.get(&a).unwrap_or(&0.))
                        .then(resources[b].speed.total_cmp(&resources[a].speed)),
                    ResourceCriterion::MaxAvailableCores => resources[b]
                        .cores_available
                        .cmp(&resources[a].cores_available)
                        .then(resources[b].speed.total_cmp(&resources[a].speed)),
                    ResourceCriterion::MaxAvailableMemory => resources[b]
                        .memory_available
                        .cmp(&resources[a].memory_available)
                        .then(resources[b].speed.total_cmp(&resources[a].speed)),
                    ResourceCriterion::MinAvailableCores => resources[a]
                        .cores_available
                        .cmp(&resources[b].cores_available)
                        .then(resources[b].speed.total_cmp(&resources[a].speed)),
                    ResourceCriterion::MinAvailableMemory => resources[a]
                        .memory_available
                        .cmp(&resources[b].memory_available)
                        .then(resources[b].speed.total_cmp(&resources[a].speed)),
                    ResourceCriterion::DotProduct => dot_product(dag.get_task(task), &resources[b])
                        .total_cmp(&dot_product(dag.get_task(task), &resources[a]))
                        .then(resources[b].speed.total_cmp(&resources[a].speed)),
                    ResourceCriterion::DotProductSpeed => (dot_product(dag.get_task(task), &resources[b])
                        + resources[b].norm_speed)
                        .total_cmp(&(dot_product(dag.get_task(task), &resources[a]) + resources[a].norm_speed)),
                })
                .unwrap();

            let get_max_cores_for_efficiency = |efficiency: f64| -> u32 {
                for cores in (1..=resources[best_resource].cores_available).rev() {
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

    /// Schedules tasks by iterating over resources and selecting the best task matching the resource.
    fn schedule_by_resource(&mut self, dag: &DAG, system: System, ctx: &SimulationContext) -> Vec<Action> {
        let rank_weight = match self.strategy.task_criterion {
            TaskCriterion::RankPack0 => 0.,
            TaskCriterion::RankPack025 => 0.25,
            TaskCriterion::RankPack05 => 0.5,
            TaskCriterion::RankPack075 => 0.75,
            TaskCriterion::RankPack1 => 1.,
            _ => 0.,
        };
        let dot_weight = 1. - rank_weight;

        let max_speed = system
            .resources
            .iter()
            .max_by(|a, b| a.speed.total_cmp(&b.speed))
            .unwrap()
            .speed;
        let mut resources: Vec<Resource> = system
            .resources
            .iter()
            .enumerate()
            .filter(|(_id, resource)| resource.cores_available > 0)
            .map(|(id, resource)| Resource {
                id,
                cores: resource.cores,
                cores_available: resource.cores_available,
                memory: resource.memory,
                memory_available: resource.memory_available,
                speed: resource.speed,
                norm_speed: resource.speed / max_speed,
            })
            .collect();
        let mut result: Vec<Action> = Vec::new();

        let avg_net_time = system.avg_net_time(ctx.id(), &DataTransferMode::Direct);
        let task_ranks = calc_ranks(system.avg_flop_time(), avg_net_time, dag);
        let mut max_rank = 0.;
        for &rank in task_ranks.iter() {
            if rank > max_rank {
                max_rank = rank;
            }
        }
        let task_ranks: Vec<f64> = task_ranks.iter().map(|r| r / max_rank).collect();

        let mut ready_tasks = dag.get_ready_tasks().clone();

        resources.sort_by(|a, b| {
            b.speed
                .total_cmp(&a.speed)
                .then(b.cores_available.cmp(&a.cores_available))
        });
        for resource in resources.iter_mut() {
            loop {
                let best_task: Option<&usize> = if self.strategy.task_criterion == TaskCriterion::RankPackSel {
                    ready_tasks
                        .iter()
                        .filter(|&t| {
                            resource.cores_available >= dag.get_task(*t).min_cores
                                && resource.memory_available >= dag.get_task(*t).memory
                        })
                        .sorted_by(|&a, &b| task_ranks[*b].total_cmp(&task_ranks[*a]))
                        .take(10)
                        .min_by(|&a, &b| {
                            dot_product(dag.get_task(*b), resource)
                                .total_cmp(&(dot_product(dag.get_task(*a), resource)))
                                .then(dag.get_task(*b).flops.total_cmp(&dag.get_task(*a).flops))
                        })
                } else {
                    ready_tasks
                        .iter()
                        .filter(|&t| {
                            resource.cores_available >= dag.get_task(*t).min_cores
                                && resource.memory_available >= dag.get_task(*t).memory
                        })
                        .min_by(|&a, &b| match self.strategy.task_criterion {
                            TaskCriterion::RankPackMult => (task_ranks[*b] * dot_product(dag.get_task(*b), resource))
                                .total_cmp(&(task_ranks[*a] * dot_product(dag.get_task(*a), resource)))
                                .then(dag.get_task(*b).flops.total_cmp(&dag.get_task(*a).flops)),
                            _ => (task_ranks[*b] * rank_weight + dot_product(dag.get_task(*b), resource) * dot_weight)
                                .total_cmp(
                                    &(task_ranks[*a] * rank_weight
                                        + dot_product(dag.get_task(*a), resource) * dot_weight),
                                )
                                .then(dag.get_task(*b).flops.total_cmp(&dag.get_task(*a).flops)),
                        })
                };
                if best_task.is_none() {
                    break;
                }
                let task_id = *best_task.unwrap();
                let task = dag.get_task(task_id);
                ready_tasks.remove(&task_id);

                let get_max_cores_for_efficiency = |efficiency: f64| -> u32 {
                    for cores in (1..=resource.cores_available).rev() {
                        let cur = task.cores_dependency.speedup(cores) / cores as f64;
                        if cur >= efficiency {
                            return cores;
                        }
                    }
                    1
                };

                let cores = match self.strategy.cores_criterion {
                    CoresCriterion::MaxCores => resource.cores_available,
                    CoresCriterion::Efficiency90 => get_max_cores_for_efficiency(0.9),
                    CoresCriterion::Efficiency50 => get_max_cores_for_efficiency(0.5),
                };
                let cores = cores.clamp(task.min_cores, task.max_cores);

                resource.cores_available -= cores;
                resource.memory_available -= task.memory;
                result.push(Action::ScheduleTask {
                    task: task_id,
                    resource: resource.id,
                    cores,
                    expected_span: None,
                });
                for &data_item in task.outputs.iter() {
                    self.data_location.insert(data_item, resource.id);
                }
            }
        }

        result
    }
}

fn dot_product(task: &Task, resource: &Resource) -> f64 {
    // When computing the dot product, task requirements and available resources
    // are normalized by the resource capacity
    let mut result = (resource.cores_available * task.max_cores) as f64 / resource.cores.pow(2) as f64;
    if resource.memory > 0 {
        result += (resource.memory_available * task.memory) as f64 / resource.memory.pow(2) as f64;
    }
    result / 2.
}

impl Scheduler for DynamicListScheduler {
    fn start(&mut self, dag: &DAG, system: System, config: Config, ctx: &SimulationContext) -> Vec<Action> {
        assert_ne!(
            config.data_transfer_mode,
            DataTransferMode::Manual,
            "DynamicListScheduler doesn't support DataTransferMode::Manual"
        );
        if self.strategy.task_criterion.to_string().starts_with("RankPack") {
            self.schedule_by_resource(dag, system, ctx)
        } else {
            self.schedule_by_task(dag, system, ctx)
        }
    }

    fn on_task_state_changed(
        &mut self,
        _task: usize,
        _task_state: TaskState,
        dag: &DAG,
        system: System,
        ctx: &SimulationContext,
    ) -> Vec<Action> {
        if self.strategy.task_criterion.to_string().starts_with("RankPack") {
            self.schedule_by_resource(dag, system, ctx)
        } else {
            self.schedule_by_task(dag, system, ctx)
        }
    }

    fn is_static(&self) -> bool {
        false
    }
}
