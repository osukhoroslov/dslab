use std::collections::{BTreeSet, HashSet};

use dslab_core::context::SimulationContext;
use dslab_core::{log_debug, log_info, log_warn};

use crate::dag::DAG;
use crate::data_item::{DataTransferMode, DataTransferStrategy};
use crate::runner::Config;
use crate::scheduler::{Action, Scheduler};
use crate::schedulers::common::{calc_ranks, evaluate_assignment, predecessors, ScheduledTask};
use crate::system::System;
use crate::task::*;

pub struct DlsScheduler {
    data_transfer_strategy: DataTransferStrategy,
}

impl DlsScheduler {
    pub fn new() -> Self {
        DlsScheduler {
            data_transfer_strategy: DataTransferStrategy::Eager,
        }
    }

    pub fn with_data_transfer_strategy(mut self, data_transfer_strategy: DataTransferStrategy) -> Self {
        self.data_transfer_strategy = data_transfer_strategy;
        self
    }
}

impl Scheduler for DlsScheduler {
    fn start(&mut self, dag: &DAG, system: System, config: Config, ctx: &SimulationContext) -> Vec<Action> {
        assert_ne!(
            config.data_transfer_mode,
            DataTransferMode::Manual,
            "DlsScheduler doesn't support DataTransferMode::Manual"
        );

        if dag.get_tasks().iter().any(|task| task.min_cores != task.max_cores) {
            log_warn!(
                ctx,
                "some tasks support different number of cores, but DLS will always use min_cores"
            );
        }

        let resources = system.resources;
        let network = system.network;

        let data_transfer_mode = &config.data_transfer_mode;

        let avg_net_time = system.avg_net_time(ctx.id(), data_transfer_mode);
        let avg_upload_net_time = system.avg_upload_net_time(ctx.id());

        let total_tasks = dag.get_tasks().len();

        let pred = predecessors(dag);

        let task_ranks = calc_ranks(system.avg_flop_time(), avg_net_time, dag);
        let mut tasks = (0..total_tasks).collect::<Vec<_>>();
        tasks.sort_by(|&a, &b| task_ranks[b].total_cmp(&task_ranks[a]));

        let mut scheduled_tasks = resources
            .iter()
            .map(|resource| {
                (0..resource.cores_available)
                    .map(|_| BTreeSet::<ScheduledTask>::new())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let mut eft = vec![1e100; total_tasks];
        let mut scheduled = vec![false; total_tasks];

        let mut result: Vec<(f64, Action)> = Vec::new();

        let inputs: HashSet<usize> = dag
            .get_tasks()
            .iter()
            .flat_map(|task| task.inputs.iter())
            .cloned()
            .collect();
        let outputs: HashSet<usize> = dag
            .get_tasks()
            .iter()
            .flat_map(|task| task.outputs.iter())
            .cloned()
            .collect();

        for _ in 0..tasks.len() {
            let mut best_pair: Option<(usize, usize)> = None;
            let mut best_dl: f64 = f64::MIN;
            let mut best_finish = -1.;
            let mut best_time = -1.;
            let mut best_cores: Vec<u32> = Vec::new();
            for &task in tasks.iter().filter(|&i| !scheduled[*i]) {
                let est = match self.data_transfer_strategy {
                    DataTransferStrategy::Eager => pred[task]
                        .iter()
                        .map(|&(task, weight)| eft[task] + weight * avg_net_time)
                        .max_by(|a, b| a.total_cmp(&b))
                        .unwrap_or(0.),
                    DataTransferStrategy::Lazy => pred[task]
                        .iter()
                        .map(|&(task, weight)| {
                            let data_upload_time = match data_transfer_mode {
                                DataTransferMode::ViaMasterNode => weight * avg_upload_net_time,
                                DataTransferMode::Direct => 0.,
                                DataTransferMode::Manual => 0.,
                            };
                            eft[task] + data_upload_time
                        })
                        .max_by(|a, b| a.total_cmp(&b))
                        .unwrap_or(0.),
                };

                for resource in 0..resources.len() {
                    let res = evaluate_assignment(
                        task,
                        resource,
                        est,
                        &scheduled_tasks,
                        &outputs,
                        avg_net_time,
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

                    let current_score = task_ranks[task] - est;
                    if current_score > best_dl {
                        best_dl = current_score;
                        best_pair = Some((task, resource));
                        best_time = time;
                        best_finish = est + time;
                        best_cores = cores;
                    }
                }
            }

            let (task, resource) = best_pair.unwrap();

            log_debug!(
                ctx,
                "scheduling [dls] task {} on resource {} on cores {:?} on time {:.3}-{:.3}",
                dag.get_task(task).name,
                resources[resource].name,
                best_cores,
                best_finish - best_time,
                best_finish
            );

            for &core in best_cores.iter() {
                scheduled_tasks[resource][core as usize].insert(ScheduledTask::new(
                    best_finish - best_time,
                    best_finish,
                    task,
                ));
            }
            eft[task] = best_finish;
            scheduled[task] = true;
            result.push((
                best_finish - best_time,
                Action::ScheduleTaskOnCores {
                    task,
                    resource: resource,
                    cores: best_cores,
                },
            ));
        }

        log_info!(
            ctx,
            "expected makespan: {:.3}",
            scheduled_tasks
                .iter()
                .enumerate()
                .map(|(resource, cores)| {
                    let cur_net_time = 1. / network.bandwidth(resources[resource].id, ctx.id());
                    cores
                        .iter()
                        .map(|schedule| {
                            schedule.iter().next_back().map_or(0., |task| {
                                task.end_time
                                    + dag
                                        .get_task(task.task)
                                        .outputs
                                        .iter()
                                        .filter(|f| !inputs.contains(f))
                                        .map(|&f| dag.get_data_item(f).size as f64 * cur_net_time)
                                        .max_by(|a, b| a.total_cmp(&b))
                                        .unwrap_or(0.)
                            })
                        })
                        .max_by(|a, b| a.total_cmp(&b))
                        .unwrap_or(0.)
                })
                .max_by(|a, b| a.total_cmp(&b))
                .unwrap_or(0.)
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
