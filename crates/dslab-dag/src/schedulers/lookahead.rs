use std::collections::{BTreeSet, HashMap};
use std::str::FromStr;

use dslab_core::context::SimulationContext;
use dslab_core::log_warn;
use dslab_core::Id;

use crate::dag::DAG;
use crate::data_item::{DataTransferMode, DataTransferStrategy};
use crate::runner::Config;
use crate::scheduler::{Action, Scheduler, SchedulerParams, TimeSpan};
use crate::schedulers::common::*;
use crate::schedulers::treap::Treap;
use crate::system::System;

pub enum DepthMode {
    /// Consider all unscheduled tasks when evaluating task assignment.
    Global,
    /// Consider only children of the current task when evaluating an assignment.
    Local,
}

impl FromStr for DepthMode {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Global" => Ok(DepthMode::Global),
            "Local" => Ok(DepthMode::Local),
            _ => Err(()),
        }
    }
}

pub struct LookaheadScheduler {
    data_transfer_strategy: DataTransferStrategy,
    depth: usize,
    depth_mode: DepthMode,
}

impl LookaheadScheduler {
    pub fn new() -> Self {
        Self {
            data_transfer_strategy: DataTransferStrategy::Eager,
            depth: usize::MAX,
            depth_mode: DepthMode::Global,
        }
    }

    pub fn from_params(params: &SchedulerParams) -> Self {
        Self {
            data_transfer_strategy: params
                .get("data_transfer_strategy")
                .unwrap_or(DataTransferStrategy::Eager),
            depth: params.get("depth").unwrap_or(usize::MAX),
            depth_mode: params.get("depth_mode").unwrap_or(DepthMode::Global),
        }
    }

    pub fn with_data_transfer_strategy(mut self, data_transfer_strategy: DataTransferStrategy) -> Self {
        self.data_transfer_strategy = data_transfer_strategy;
        self
    }

    fn schedule(&self, dag: &DAG, system: System, config: Config, ctx: &SimulationContext) -> Vec<Action> {
        let resources = system.resources;
        let network = system.network;

        let avg_net_time = system.avg_net_time(ctx.id(), &config.data_transfer_mode);

        let task_count = dag.get_tasks().len();

        let task_ranks = calc_ranks(system.avg_flop_time(), avg_net_time, dag);
        let mut task_ids = (0..task_count).collect::<Vec<_>>();
        task_ids.sort_by(|&a, &b| task_ranks[b].total_cmp(&task_ranks[a]));

        let mut scheduled = vec![false; task_count];
        let mut task_finish_times = vec![0.; task_count];
        let mut scheduled_tasks: Vec<Vec<BTreeSet<ScheduledTask>>> = resources
            .iter()
            .map(|resource| (0..resource.cores_available).map(|_| BTreeSet::new()).collect())
            .collect();
        let mut memory_usage: Vec<Treap> = (0..resources.len()).map(|_| Treap::new()).collect();
        let mut data_locations: HashMap<usize, Id> = HashMap::new();
        let mut task_locations: HashMap<usize, Id> = HashMap::new();

        let mut result: Vec<(f64, Action)> = Vec::new();

        for &task_id in task_ids.iter() {
            let mut best_makespan = f64::MAX;
            let mut best_start = -1.;
            let mut best_finish = -1.;
            let mut best_resource = 0;
            let mut best_cores: Vec<u32> = Vec::new();
            for resource in 0..resources.len() {
                let res = evaluate_assignment(
                    task_id,
                    resource,
                    &task_finish_times,
                    &scheduled_tasks,
                    &memory_usage,
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
                memory_usage[resource].add(start_time, finish_time, dag.get_task(task_id).memory);
                task_finish_times[task_id] = finish_time;
                scheduled[task_id] = true;
                let mut output_time: f64 = 0.;
                for &output in dag.get_task(task_id).outputs.iter() {
                    data_locations.insert(output, resources[resource].id);
                }
                task_locations.insert(task_id, resources[resource].id);
                to_undo.push((
                    resource,
                    cores.clone(),
                    ScheduledTask::new(start_time, finish_time, task_id),
                ));

                let mut depth = vec![usize::MAX; task_ids.len()];
                depth[task_id] = 0;
                for &task in task_ids.iter() {
                    match self.depth_mode {
                        DepthMode::Global => {
                            if scheduled[task] {
                                depth[task] = 0;
                            } else if depth[task] == usize::MAX {
                                // a task can have zero predecessors, then depth should be 1
                                depth[task] = 1;
                            }
                        }
                        DepthMode::Local => {}
                    }
                    for (child, _weight) in task_successors(task, dag) {
                        depth[child] = depth[child].min(depth[task].saturating_add(1));
                    }
                }

                let mut unscheduled_tasks = Vec::new();
                for &task in task_ids.iter() {
                    if !scheduled[task] && depth[task] <= self.depth {
                        unscheduled_tasks.push(task);
                    }
                }

                let mut makespan = finish_time + output_time;

                for child in unscheduled_tasks.into_iter() {
                    let (resource, cores, start, finish) = {
                        let task = child;

                        let mut best_start = -1.;
                        let mut best_finish = -1.;
                        let mut best_resource = 0;
                        let mut best_cores: Vec<u32> = Vec::new();
                        for resource in 0..resources.len() {
                            let res = evaluate_assignment(
                                task,
                                resource,
                                &task_finish_times,
                                &scheduled_tasks,
                                &memory_usage,
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

                        assert_ne!(best_finish, -1.);

                        (best_resource, best_cores, best_start, best_finish)
                    };

                    scheduled[child] = true;
                    task_finish_times[child] = finish;
                    for &core in cores.iter() {
                        scheduled_tasks[resource][core as usize].insert(ScheduledTask::new(start, finish, child));
                    }
                    memory_usage[resource].add(start, finish, dag.get_task(child).memory);
                    output_time = 0.;
                    for &output in dag.get_task(child).outputs.iter() {
                        data_locations.insert(output, resources[resource].id);
                    }
                    task_locations.insert(child, resources[resource].id);

                    to_undo.push((resource, cores, ScheduledTask::new(start, finish, child)));

                    makespan = makespan.max(finish + output_time);

                    if makespan >= best_makespan {
                        break;
                    }
                }

                for (resource, cores, scheduled_task) in to_undo.into_iter() {
                    scheduled[scheduled_task.task] = false;
                    for &core in cores.iter() {
                        assert!(scheduled_tasks[resource][core as usize].remove(&scheduled_task));
                    }
                    memory_usage[resource].remove(
                        scheduled_task.start_time,
                        scheduled_task.finish_time,
                        dag.get_task(scheduled_task.task).memory,
                    );
                }
                data_locations = old_data_location;
                task_locations = old_task_location;

                if best_makespan > makespan {
                    best_start = start_time;
                    best_finish = finish_time;
                    best_makespan = makespan;
                    best_resource = resource;
                    best_cores = cores.clone();
                }
            }

            assert_ne!(best_finish, -1.);

            scheduled[task_id] = true;
            task_finish_times[task_id] = best_finish;
            for &core in best_cores.iter() {
                scheduled_tasks[best_resource][core as usize].insert(ScheduledTask::new(
                    best_start,
                    best_finish,
                    task_id,
                ));
            }
            for &output in dag.get_task(task_id).outputs.iter() {
                data_locations.insert(output, resources[best_resource].id);
            }
            task_locations.insert(task_id, resources[best_resource].id);
            memory_usage[best_resource].add(best_start, best_finish, dag.get_task(task_id).memory);

            result.push((
                best_start,
                Action::ScheduleTaskOnCores {
                    task: task_id,
                    resource: best_resource,
                    cores: best_cores,
                    expected_span: Some(TimeSpan::new(best_start, best_finish)),
                },
            ));
        }

        result.sort_by(|a, b| a.0.total_cmp(&b.0));
        result.into_iter().map(|(_, b)| b).collect()
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

        self.schedule(dag, system, config, ctx)
    }

    fn is_static(&self) -> bool {
        true
    }
}

impl Default for LookaheadScheduler {
    fn default() -> Self {
        Self::new()
    }
}
