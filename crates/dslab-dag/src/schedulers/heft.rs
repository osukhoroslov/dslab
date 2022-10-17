use std::collections::{BTreeSet, HashMap};

use dslab_core::context::SimulationContext;
use dslab_core::Id;
use dslab_core::{log_debug, log_error, log_info, log_warn};

use crate::dag::DAG;
use crate::data_item::{DataTransferMode, DataTransferStrategy};
use crate::runner::Config;
use crate::scheduler::{Action, Scheduler};
use crate::schedulers::common::*;
use crate::system::System;
use crate::task::*;

pub struct HeftScheduler {
    data_transfer_strategy: DataTransferStrategy,
}

impl HeftScheduler {
    pub fn new() -> Self {
        HeftScheduler {
            data_transfer_strategy: DataTransferStrategy::Eager,
        }
    }

    pub fn with_data_transfer_strategy(mut self, data_transfer_strategy: DataTransferStrategy) -> Self {
        self.data_transfer_strategy = data_transfer_strategy;
        self
    }
}

impl Scheduler for HeftScheduler {
    fn start(&mut self, dag: &DAG, system: System, config: Config, ctx: &SimulationContext) -> Vec<Action> {
        assert_ne!(
            config.data_transfer_mode,
            DataTransferMode::Manual,
            "HeftScheduler doesn't support DataTransferMode::Manual"
        );

        if dag.get_tasks().iter().any(|task| task.min_cores != task.max_cores) {
            log_warn!(
                ctx,
                "some tasks support different number of cores, but HEFT will always use min_cores"
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

        let mut data_location: HashMap<usize, Id> = HashMap::new();
        let mut task_location: HashMap<usize, Id> = HashMap::new();

        let mut result: Vec<(f64, Action)> = Vec::new();

        for task_id in task_ids.into_iter() {
            let mut best_finish = -1.;
            let mut best_start = -1.;
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

                if best_finish == -1. || best_finish > est + time {
                    best_start = est;
                    best_finish = est + time;
                    best_resource = resource;
                    best_cores = cores;
                }
            }

            if best_finish == -1. {
                log_error!(
                    ctx,
                    "couldn't schedule task {}, since every resource has less cores than minimum requirement for this task",
                    dag.get_task(task_id ).name
                );
                return Vec::new();
            }

            log_debug!(
                ctx,
                "scheduling [heft] task {} on resource {} on cores {:?} on time {:.3}-{:.3}",
                dag.get_task(task_id).name,
                resources[best_resource].name,
                best_cores,
                best_start,
                best_finish
            );
            for &core in best_cores.iter() {
                scheduled_tasks[best_resource][core as usize].insert(ScheduledTask::new(
                    best_start,
                    best_finish,
                    task_id,
                ));
            }
            eft[task_id] = best_finish;
            result.push((
                best_start,
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
