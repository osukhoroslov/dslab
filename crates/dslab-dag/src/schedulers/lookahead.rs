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

        let avg_net_time = system.avg_net_time(ctx.id(), &config.data_transfer_mode);

        let task_count = dag.get_tasks().len();

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
        let mut task_finish_times = vec![0.; task_count];
        let mut scheduled = vec![false; task_count];

        let mut data_locations: HashMap<usize, Id> = HashMap::new();
        let mut task_locations: HashMap<usize, Id> = HashMap::new();

        let mut result: Vec<(f64, Action)> = Vec::new();

        for task_id in task_ids.into_iter() {
            let mut best_makespan = -1.;
            let mut best_start = -1.;
            let mut best_finish = -1.;
            let mut best_resource = 0 as usize;
            let mut best_cores: Vec<u32> = Vec::new();
            for resource in 0..resources.len() {
                let res = evaluate_assignment(
                    task_id,
                    resource,
                    &task_finish_times,
                    &scheduled_tasks,
                    &data_locations,
                    &task_locations,
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
                let (start_time, finish_time, cores) = res.unwrap();

                let mut to_undo: Vec<(usize, Vec<u32>, ScheduledTask)> = Vec::new();
                let old_task_location = task_locations.clone();
                let old_data_location = data_locations.clone();

                for &core in cores.iter() {
                    scheduled_tasks[resource][core as usize].insert(ScheduledTask::new(
                        start_time,
                        finish_time,
                        task_id,
                    ));
                }
                task_finish_times[task_id] = finish_time;
                scheduled[task_id] = true;
                for &output in dag.get_task(task_id).outputs.iter() {
                    data_locations.insert(output, resources[resource].id);
                }
                task_locations.insert(task_id, resources[resource].id);
                to_undo.push((
                    resource,
                    cores.clone(),
                    ScheduledTask::new(start_time, finish_time, task_id),
                ));

                let mut unscheduled_tasks = (0..task_count).filter(|&task| !scheduled[task]).collect::<Vec<usize>>();
                unscheduled_tasks.sort_by(|&a, &b| task_ranks[b].total_cmp(&task_ranks[a]));
                let mut makespan = finish_time;
                for &child in unscheduled_tasks.iter() {
                    let (resource, cores, start, finish) = {
                        let task = child;

                        let mut best_start = -1.;
                        let mut best_finish = -1.;
                        let mut best_resource = 0 as usize;
                        let mut best_cores: Vec<u32> = Vec::new();
                        for resource in 0..resources.len() {
                            let res = evaluate_assignment(
                                task,
                                resource,
                                &task_finish_times,
                                &scheduled_tasks,
                                &data_locations,
                                &task_locations,
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
                            let (start_time, finish_time, cores) = res.unwrap();

                            if best_finish == -1. || best_finish > finish_time {
                                best_start = start_time;
                                best_finish = finish_time;
                                best_resource = resource;
                                best_cores = cores;
                            }
                        }

                        assert!(best_finish != -1.);

                        (best_resource, best_cores, best_start, best_finish)
                    };
                    for &core in cores.iter() {
                        scheduled_tasks[resource][core as usize].insert(ScheduledTask::new(start, finish, child));
                    }
                    task_finish_times[child] = finish;
                    scheduled[child] = true;
                    for &output in dag.get_task(child).outputs.iter() {
                        data_locations.insert(output, resources[resource].id);
                    }
                    task_locations.insert(child, resources[resource].id);
                    to_undo.push((resource, cores, ScheduledTask::new(start, finish, child)));
                    if finish > makespan {
                        makespan = finish;
                    }
                }

                for (resource, cores, scheduled_task) in to_undo.into_iter() {
                    for &core in cores.iter() {
                        assert!(scheduled_tasks[resource][core as usize].remove(&scheduled_task));
                    }
                    scheduled[scheduled_task.task] = false;
                }
                data_locations = old_data_location;
                task_locations = old_task_location;

                if best_makespan == -1. || best_makespan > makespan {
                    best_start = start_time;
                    best_finish = finish_time;
                    best_makespan = makespan;
                    best_resource = resource;
                    best_cores = cores.clone();
                }
            }

            assert!(best_finish != -1.);

            for &core in best_cores.iter() {
                scheduled_tasks[best_resource][core as usize].insert(ScheduledTask::new(
                    best_start,
                    best_finish,
                    task_id,
                ));
            }
            task_finish_times[task_id] = best_finish;
            scheduled[task_id] = true;
            result.push((
                best_start,
                Action::ScheduleTaskOnCores {
                    task: task_id,
                    resource: best_resource,
                    cores: best_cores,
                    expected_span: Some((best_start, best_finish)),
                },
            ));
            for &output in dag.get_task(task_id).outputs.iter() {
                data_locations.insert(output, resources[best_resource].id);
            }
            task_locations.insert(task_id, resources[best_resource].id);
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
