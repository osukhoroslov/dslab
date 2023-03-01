use std::collections::HashMap;

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
    speed: u64,
}

enum TaskCriterion {
    BottomLevel,
    ChildrenCount,
    DataSize,
    ComputationSize,
}

#[derive(PartialEq)]
enum ClusterCriterion {
    TaskData,
    IdleCores,
    Speed,
}

enum CoresCriterion {
    Efficiency90,
    Efficiency50,
    MaxCores,
}

pub struct PortfolioScheduler {
    task_criterion: TaskCriterion,
    cluster_criterion: ClusterCriterion,
    cores_criterion: CoresCriterion,
    data_location: HashMap<usize, usize>,
}

impl PortfolioScheduler {
    pub fn new(algo: usize) -> Self {
        PortfolioScheduler {
            task_criterion: match algo / 9 {
                0 => TaskCriterion::BottomLevel,
                1 => TaskCriterion::ChildrenCount,
                2 => TaskCriterion::DataSize,
                3 => TaskCriterion::ComputationSize,
                _ => {
                    eprintln!("Wrong algo {}", algo);
                    std::process::exit(1);
                }
            },
            cluster_criterion: match algo % 9 / 3 {
                0 => ClusterCriterion::TaskData,
                1 => ClusterCriterion::IdleCores,
                2 => ClusterCriterion::Speed,
                _ => {
                    eprintln!("Wrong algo {}", algo);
                    std::process::exit(1);
                }
            },
            cores_criterion: match algo % 3 {
                0 => CoresCriterion::Efficiency90,
                1 => CoresCriterion::Efficiency50,
                2 => CoresCriterion::MaxCores,
                _ => {
                    eprintln!("Wrong algo {}", algo);
                    std::process::exit(1);
                }
            },
            data_location: HashMap::new(),
        }
    }

    pub fn from_params(params: &SchedulerParams) -> Self {
        Self::new(params.get("algo").unwrap())
    }

    fn schedule(&mut self, dag: &DAG, resources: &[crate::resource::Resource]) -> Vec<Action> {
        let mut resources: Vec<Resource> = resources
            .iter()
            .map(|resource| Resource {
                cores_available: resource.cores_available,
                memory_available: resource.memory_available,
                speed: resource.speed,
            })
            .collect();
        let mut result: Vec<Action> = Vec::new();

        let rank = calc_ranks(1., 0., dag);

        let get_data_size = |task_id: usize| -> u64 {
            let task = dag.get_task(task_id);

            let mut data_items = task.inputs.clone();
            data_items.extend(task.outputs.clone());
            data_items
                .into_iter()
                .map(|data_item| dag.get_data_item(data_item).size)
                .sum::<u64>()
        };

        let mut ready_tasks = dag.get_ready_tasks().iter().cloned().collect::<Vec<usize>>();
        ready_tasks.sort_by(|&a, &b| match self.task_criterion {
            TaskCriterion::BottomLevel => rank[b].total_cmp(&rank[a]),
            TaskCriterion::ChildrenCount => dag.get_task(b).outputs.len().cmp(&dag.get_task(a).outputs.len()),
            TaskCriterion::DataSize => get_data_size(b).cmp(&get_data_size(a)),
            TaskCriterion::ComputationSize => dag.get_task(b).flops.cmp(&dag.get_task(a).flops),
        });

        for task in ready_tasks.into_iter() {
            let mut total_task_data: HashMap<usize, u64> = HashMap::new();
            if self.cluster_criterion == ClusterCriterion::TaskData {
                for input in dag.get_task(task).inputs.iter() {
                    if let Some(location) = self.data_location.get(input) {
                        *total_task_data.entry(*location).or_default() += dag.get_data_item(*input).size;
                    }
                }
            }

            let best_resource = (0..resources.len())
                .filter(|&r| {
                    resources[r].cores_available >= dag.get_task(task).min_cores
                        && resources[r].memory_available >= dag.get_task(task).memory
                })
                .min_by(|&a, &b| match self.cluster_criterion {
                    ClusterCriterion::TaskData => total_task_data
                        .get(&b)
                        .unwrap_or(&0)
                        .cmp(total_task_data.get(&a).unwrap_or(&0)),
                    ClusterCriterion::IdleCores => resources[b].cores_available.cmp(&resources[a].cores_available),
                    ClusterCriterion::Speed => resources[b].speed.cmp(&resources[a].speed),
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

            let cores = match self.cores_criterion {
                CoresCriterion::Efficiency90 => get_max_cores_for_efficiency(0.9),
                CoresCriterion::Efficiency50 => get_max_cores_for_efficiency(0.5),
                CoresCriterion::MaxCores => resources[best_resource].cores_available,
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

impl Scheduler for PortfolioScheduler {
    fn start(&mut self, dag: &DAG, system: System, config: Config, _ctx: &SimulationContext) -> Vec<Action> {
        assert_ne!(
            config.data_transfer_mode,
            DataTransferMode::Manual,
            "PortfolioScheduler doesn't support DataTransferMode::Manual"
        );
        self.schedule(dag, system.resources)
    }

    fn on_task_state_changed(
        &mut self,
        _task: usize,
        _task_state: TaskState,
        dag: &DAG,
        system: System,
        _ctx: &SimulationContext,
    ) -> Vec<Action> {
        self.schedule(dag, system.resources)
    }

    fn is_static(&self) -> bool {
        false
    }
}
