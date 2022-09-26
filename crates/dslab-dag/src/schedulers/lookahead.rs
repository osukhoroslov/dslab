use std::collections::{BTreeSet, HashSet};

use dslab_core::context::SimulationContext;
use dslab_core::{log_debug, log_info, log_warn};
use dslab_network::network::Network;

use crate::dag::DAG;
use crate::data_item::{DataTransferMode, DataTransferStrategy};
use crate::runner::Config;
use crate::scheduler::{Action, Scheduler};
use crate::schedulers::common::{assign_task_on_resource, calc_ranks, successors, ScheduledTask};
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
    fn start(
        &mut self,
        dag: &DAG,
        resources: &Vec<crate::resource::Resource>,
        network: &Network,
        config: Config,
        ctx: &SimulationContext,
    ) -> Vec<Action> {
        assert_ne!(
            config.data_transfer_mode,
            DataTransferMode::Manual,
            "LookaheadScheduler doesn't support DataTransferMode::Manual"
        );

        if dag.get_tasks().iter().any(|task| task.min_cores != task.max_cores) {
            log_warn!(
                ctx,
                "some tasks support different number of cores, but HEFT will always use min_cores"
            );
        }

        let data_transfer_mode = &config.data_transfer_mode;

        // average time over all resources for executing one flop
        let avg_flop_time = resources.iter().map(|r| 1. / r.speed as f64).sum::<f64>() / resources.len() as f64;

        let avg_net_time = resources
            .iter()
            .map(|r1| {
                resources
                    .iter()
                    .map(|r2| data_transfer_mode.net_time(network, r1.id, r2.id, ctx.id()))
                    .sum::<f64>()
            })
            .sum::<f64>()
            / (resources.len() as f64).powf(2.);
        let avg_upload_net_time = resources
            .iter()
            .map(|r| 1. / network.bandwidth(r.id, ctx.id()))
            .sum::<f64>()
            / resources.len() as f64;

        let total_tasks = dag.get_tasks().len();

        let rank = calc_ranks(avg_flop_time, avg_net_time, dag);

        let mut pred = vec![vec![(0 as usize, 0.); 0]; total_tasks];
        for task in 0..total_tasks {
            for &(succ, weight) in successors(task, dag).iter() {
                pred[succ].push((task, weight as f64));
            }
        }

        let mut tasks = (0..total_tasks).collect::<Vec<_>>();
        tasks.sort_by(|&a, &b| rank[b].total_cmp(&rank[a]));

        let mut scheduled_tasks = resources
            .iter()
            .map(|resource| {
                (0..resource.cores_available)
                    .map(|_| BTreeSet::<ScheduledTask>::new())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let mut eft = vec![0.; total_tasks];
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

        let get_task_est = |task: usize, eft: &Vec<f64>| -> f64 {
            match self.data_transfer_strategy {
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
            }
        };

        for task in tasks.into_iter() {
            let est = get_task_est(task, &eft);

            let mut best_eft = -1.;
            let mut best_time = -1.;
            let mut best_finish = -1.;
            let mut best_resource = 0 as usize;
            let mut best_cores: Vec<u32> = Vec::new();
            for resource in 0..resources.len() {
                let res = assign_task_on_resource(
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

                let mut to_undo: Vec<(usize, Vec<u32>, ScheduledTask)> = Vec::new();

                for &core in cores.iter() {
                    scheduled_tasks[resource][core as usize].insert(ScheduledTask::new(est, est + time, task));
                }
                eft[task] = est + time;
                scheduled[task] = true;
                to_undo.push((resource, cores.clone(), ScheduledTask::new(est, est + time, task)));

                let mut children = (0..total_tasks)
                    .filter(|&task| !scheduled[task])
                    .collect::<Vec<usize>>();
                children.sort_by(|&a, &b| rank[b].total_cmp(&rank[a]));
                let mut max_eft = est + time;
                for &child in children.iter() {
                    let (resource, cores, finish, time) = {
                        let task = child;
                        let est = get_task_est(task, &eft);

                        let mut best_finish = -1.;
                        let mut best_time = -1.;
                        let mut best_resource = 0 as usize;
                        let mut best_cores: Vec<u32> = Vec::new();
                        for resource in 0..resources.len() {
                            let res = assign_task_on_resource(
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

                if best_eft == -1. || best_eft > max_eft {
                    best_time = time;
                    best_finish = est + time;
                    best_eft = max_eft;
                    best_resource = resource;
                    best_cores = cores.clone();
                }
            }

            assert!(best_finish != -1.);

            log_debug!(
                ctx,
                "scheduling [lookahead] task {} on resource {} on cores {:?} on time {:.3}-{:.3}",
                dag.get_task(task).name,
                resources[best_resource].name,
                best_cores,
                best_finish - best_time,
                best_finish
            );

            for &core in best_cores.iter() {
                scheduled_tasks[best_resource][core as usize].insert(ScheduledTask::new(
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
                    resource: best_resource,
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
        _resources: &Vec<crate::resource::Resource>,
        _ctx: &SimulationContext,
    ) -> Vec<Action> {
        Vec::new()
    }
}
