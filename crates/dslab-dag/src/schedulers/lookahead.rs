use std::collections::{BTreeSet, HashMap};

use dslab_core::context::SimulationContext;
use dslab_core::Id;
use dslab_core::{log_info, log_warn};

use crate::dag::DAG;
use crate::data_item::{DataTransferMode, DataTransferStrategy};
use crate::runner::Config;
use crate::scheduler::{Action, Scheduler};
use crate::schedulers::common::*;
use crate::system::System;
use crate::task::*;

pub struct LookaheadScheduler {
    data_transfer_strategy: DataTransferStrategy,
}

impl LookaheadScheduler {
    pub fn new() -> Self {
        LookaheadScheduler {
            data_transfer_strategy: DataTransferStrategy::Eager,
        }
    }

    pub fn with_data_transfer_strategy(mut self, data_transfer_strategy: DataTransferStrategy) -> Self {
        self.data_transfer_strategy = data_transfer_strategy;
        self
    }
}

impl Scheduler for LookaheadScheduler {
    fn start(&mut self, dag: &DAG, system: System, config: Config, ctx: &SimulationContext) -> Vec<Action> {
        assert_ne!(
            config.data_transfer_mode,
            DataTransferMode::Manual,
            "LookaheadScheduler doesn't support DataTransferMode::Manual"
        );

        if dag.get_tasks().iter().any(|task| task.min_cores != task.max_cores) {
            log_warn!(
                ctx,
                "some tasks support different number of cores, but Lookahead will always use min_cores"
            );
        }

        let resources = system.resources;
        let network = system.network;

        let data_transfer_mode = &config.data_transfer_mode;

        let avg_net_time = system.avg_net_time(ctx.id(), data_transfer_mode);

        let task_count = dag.get_tasks().len();

        let pred = predecessors(dag);

        let task_ranks = calc_ranks(system.avg_flop_time(), avg_net_time, dag);
        let mut task_ids = (0..task_count).collect::<Vec<_>>();
        task_ids.sort_by(|&a, &b| task_ranks[b].total_cmp(&task_ranks[a]));

        let mut scheduled_tasks = resources
            .iter()
            .map(|resource| {
                (0..resource.cores_available)
                    .map(|_| BTreeSet::<ScheduledTask>::new())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let mut eft = vec![0.; task_count];
        let mut scheduled = vec![false; task_count];

        let mut data_location: HashMap<usize, Id> = HashMap::new();
        let mut task_location: HashMap<usize, Id> = HashMap::new();

        let mut result: Vec<(f64, Action)> = Vec::new();

        for task_id in task_ids.into_iter() {
            let mut best_eft = -1.;
            let mut best_time = -1.;
            let mut best_finish = -1.;
            let mut best_resource = 0 as usize;
            let mut best_cores: Vec<u32> = Vec::new();
            for resource in 0..resources.len() {
                let res = evaluate_assignment(
                    task_id,
                    resource,
                    &eft,
                    &pred,
                    &scheduled_tasks,
                    &data_location,
                    &task_location,
                    &self.data_transfer_strategy,
                    dag,
                    resources,
                    network,
                    &config,
                    ctx,
                );
                if res.is_none() {
                    continue;
                }
                let (est, time, cores) = res.unwrap();

                let mut to_undo: Vec<(usize, Vec<u32>, ScheduledTask)> = Vec::new();
                let old_task_location = task_location.clone();
                let old_data_location = data_location.clone();

                for &core in cores.iter() {
                    scheduled_tasks[resource][core as usize].insert(ScheduledTask::new(est, est + time, task_id));
                }
                eft[task_id] = est + time;
                scheduled[task_id] = true;
                for &output in dag.get_task(task_id).outputs.iter() {
                    data_location.insert(output, resources[resource].id);
                }
                task_location.insert(task_id, resources[resource].id);
                to_undo.push((resource, cores.clone(), ScheduledTask::new(est, est + time, task_id)));

                let mut children = (0..task_count).filter(|&task| !scheduled[task]).collect::<Vec<usize>>();
                children.sort_by(|&a, &b| task_ranks[b].total_cmp(&task_ranks[a]));
                let mut max_eft = est + time;
                for &child in children.iter() {
                    let (resource, cores, finish, time) = {
                        let task = child;

                        let mut best_finish = -1.;
                        let mut best_time = -1.;
                        let mut best_resource = 0 as usize;
                        let mut best_cores: Vec<u32> = Vec::new();
                        for resource in 0..resources.len() {
                            let res = evaluate_assignment(
                                task,
                                resource,
                                &eft,
                                &pred,
                                &scheduled_tasks,
                                &data_location,
                                &task_location,
                                &self.data_transfer_strategy,
                                dag,
                                resources,
                                network,
                                &config,
                                ctx,
                            );
                            if res.is_none() {
                                continue;
                            }
                            let (est, time, cores) = res.unwrap();

                            if best_finish == -1. || best_finish > est + time {
                                best_time = time;
                                best_finish = est + time;
                                best_resource = resource;
                                best_cores = cores;
                            }
                        }

                        assert!(best_finish != -1.);

                        (best_resource, best_cores, best_finish, best_time)
                    };
                    for &core in cores.iter() {
                        scheduled_tasks[resource][core as usize].insert(ScheduledTask::new(
                            finish - time,
                            finish,
                            child,
                        ));
                    }
                    eft[child] = finish;
                    scheduled[child] = true;
                    for &output in dag.get_task(child).outputs.iter() {
                        data_location.insert(output, resources[resource].id);
                    }
                    task_location.insert(child, resources[resource].id);
                    to_undo.push((resource, cores, ScheduledTask::new(finish - time, finish, child)));
                    if finish > max_eft {
                        max_eft = finish;
                    }
                }

                for (resource, cores, scheduled_task) in to_undo.into_iter() {
                    for &core in cores.iter() {
                        assert!(scheduled_tasks[resource][core as usize].remove(&scheduled_task));
                    }
                    scheduled[scheduled_task.task] = false;
                }
                data_location = old_data_location;
                task_location = old_task_location;

                if best_eft == -1. || best_eft > max_eft {
                    best_time = time;
                    best_finish = est + time;
                    best_eft = max_eft;
                    best_resource = resource;
                    best_cores = cores.clone();
                }
            }

            assert!(best_finish != -1.);

            for &core in best_cores.iter() {
                scheduled_tasks[best_resource][core as usize].insert(ScheduledTask::new(
                    best_finish - best_time,
                    best_finish,
                    task_id,
                ));
            }
            eft[task_id] = best_finish;
            scheduled[task_id] = true;
            result.push((
                best_finish - best_time,
                Action::ScheduleTaskOnCores {
                    task: task_id,
                    resource: best_resource,
                    cores: best_cores,
                },
            ));
            for &output in dag.get_task(task_id).outputs.iter() {
                data_location.insert(output, resources[best_resource].id);
            }
            task_location.insert(task_id, resources[best_resource].id);
        }

        log_info!(
            ctx,
            "expected makespan: {:.3}",
            calc_makespan(&scheduled_tasks, dag, resources, network, ctx)
        );

        result.sort_by(|a, b| a.0.total_cmp(&b.0));
        result.into_iter().map(|(_, b)| b).collect()
    }

    fn on_task_state_changed(
        &mut self,
        _task: usize,
        _task_state: TaskState,
        _dag: &DAG,
        _system: System,
        _ctx: &SimulationContext,
    ) -> Vec<Action> {
        Vec::new()
    }
}
