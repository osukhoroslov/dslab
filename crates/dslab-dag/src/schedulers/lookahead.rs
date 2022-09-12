use std::cmp::Ordering;
use std::collections::{BTreeSet, HashSet};
use std::ops::Bound::{Excluded, Included, Unbounded};

use dslab_core::context::SimulationContext;
use dslab_core::{log_debug, log_error, log_info, log_warn};
use dslab_network::network::Network;

use crate::dag::DAG;
use crate::runner::{Config, DataTransferMode};
use crate::scheduler::{Action, Scheduler};
use crate::task::*;

pub enum DataTransferStrategy {
    Eager, // default assumption in HEFT -- data transfer starts as soon as task finished
    Lazy,  // data transfer starts only when the destination node is ready to execute the task
}

#[derive(Clone, Debug)]
struct ScheduledTask {
    start_time: f64,
    end_time: f64,
    task: usize,
}

impl ScheduledTask {
    fn new(start_time: f64, end_time: f64, task: usize) -> ScheduledTask {
        ScheduledTask {
            start_time,
            end_time,
            task,
        }
    }
}

impl PartialOrd for ScheduledTask {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(
            self.start_time
                .total_cmp(&other.start_time)
                .then(self.end_time.total_cmp(&other.end_time)),
        )
    }
}

impl Ord for ScheduledTask {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl PartialEq for ScheduledTask {
    fn eq(&self, other: &Self) -> bool {
        self.start_time == other.start_time
    }
}

impl Eq for ScheduledTask {}

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

    fn successors(v: usize, dag: &DAG) -> Vec<(usize, u64)> {
        let mut result = Vec::new();
        for &data_item_id in dag.get_task(v).outputs.iter() {
            let data_item = dag.get_data_item(data_item_id);
            result.extend(data_item.consumers.iter().map(|&v| (v, data_item.size)));
        }
        result
    }

    fn calc_rank(
        &self,
        v: usize,
        avg_flop_time: f64,
        avg_net_time: f64,
        dag: &DAG,
        rank: &mut Vec<f64>,
        used: &mut Vec<bool>,
    ) {
        if used[v] {
            return;
        }
        used[v] = true;

        rank[v] = 0.;
        for &(succ, edge_weight) in LookaheadScheduler::successors(v, dag).iter() {
            self.calc_rank(succ, avg_flop_time, avg_net_time, dag, rank, used);
            rank[v] = rank[v].max(rank[succ] + edge_weight as f64 * avg_net_time);
        }
        rank[v] += dag.get_task(v).flops as f64 * avg_flop_time;
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

        if dag.get_tasks().iter().map(|task| task.min_cores).max()
            > resources.iter().map(|r| r.compute.borrow().cores_total()).max()
        {
            log_error!(ctx, "some tasks require more cores than any resource can provide");
            return Vec::new();
        }

        let data_transfer_mode = config.data_transfer_mode;

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

        let mut used = vec![false; total_tasks];
        let mut rank = vec![0.; total_tasks];

        for i in 0..total_tasks {
            self.calc_rank(i, avg_flop_time, avg_net_time, dag, &mut rank, &mut used);
        }

        let mut pred = vec![vec![(0 as usize, 0.); 0]; total_tasks];
        for task in 0..total_tasks {
            for &(succ, weight) in LookaheadScheduler::successors(task, dag).iter() {
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

        let assign_task_on_resource = |task: usize,
                                       resource: usize,
                                       est: f64,
                                       scheduled_tasks: &Vec<Vec<BTreeSet<ScheduledTask>>>|
         -> Option<(f64, f64, Vec<u32>)> {
            let cur_net_time = 1. / network.bandwidth(ctx.id(), resources[resource].id);
            let input_load_time = match self.data_transfer_strategy {
                DataTransferStrategy::Eager => dag
                    .get_task(task)
                    .inputs
                    .iter()
                    .filter(|f| !outputs.contains(f))
                    .map(|&f| dag.get_data_item(f).size as f64 * cur_net_time)
                    .max_by(|a, b| a.total_cmp(&b))
                    .unwrap_or(0.),
                DataTransferStrategy::Lazy => 0.,
            };
            let est = est.max(input_load_time);

            let need_cores = dag.get_task(task).min_cores;
            if resources[resource].compute.borrow().cores_total() < need_cores {
                return None;
            }

            let download_time = match self.data_transfer_strategy {
                DataTransferStrategy::Eager => 0.,
                DataTransferStrategy::Lazy => dag
                    .get_task(task)
                    .inputs
                    .iter()
                    .map(|&f| match data_transfer_mode {
                        DataTransferMode::ViaMasterNode => dag.get_data_item(f).size as f64 * cur_net_time,
                        DataTransferMode::Direct => {
                            if outputs.contains(&f) {
                                dag.get_data_item(f).size as f64 * avg_net_time
                            } else {
                                dag.get_data_item(f).size as f64 * cur_net_time
                            }
                        }
                        DataTransferMode::Manual => 0.,
                    })
                    .max_by(|a, b| a.total_cmp(&b))
                    .unwrap_or(0.),
            };
            let time = dag.get_task(task).flops as f64
                / resources[resource].speed as f64
                / dag.get_task(task).cores_dependency.speedup(need_cores)
                + download_time;

            let mut possible_starts = scheduled_tasks[resource]
                .iter()
                .flat_map(|schedule| schedule.iter().map(|scheduled_task| scheduled_task.end_time))
                .filter(|&a| a >= est)
                .collect::<Vec<_>>();
            possible_starts.push(est);
            possible_starts.sort_by(|a, b| a.total_cmp(&b));
            possible_starts.dedup();

            let mut cores: Vec<u32> = Vec::new();
            let mut est = est;
            for &possible_start in possible_starts.iter() {
                for core in 0..resources[resource].cores_available as usize {
                    let next = scheduled_tasks[resource][core]
                        .range((Excluded(ScheduledTask::new(possible_start, 0., 0)), Unbounded))
                        .next();
                    let prev = scheduled_tasks[resource][core]
                        .range((Unbounded, Included(ScheduledTask::new(possible_start, 0., 0))))
                        .next_back();
                    if let Some(scheduled_task) = prev {
                        if scheduled_task.end_time > possible_start {
                            continue;
                        }
                    }
                    if let Some(scheduled_task) = next {
                        if scheduled_task.start_time < possible_start + time {
                            continue;
                        }
                    }
                    cores.push(core as u32);
                }
                if cores.len() >= need_cores as usize {
                    est = possible_start;
                    break;
                } else {
                    cores.clear();
                }
            }

            assert!(cores.len() >= need_cores as usize);

            let cores = cores.iter().take(need_cores as usize).cloned().collect::<Vec<_>>();

            Some((est, time, cores))
        };

        for task in tasks.into_iter() {
            let est = get_task_est(task, &eft);

            let mut best_eft = -1.;
            let mut best_time = -1.;
            let mut best_finish = -1.;
            let mut best_resource = 0 as usize;
            let mut best_cores: Vec<u32> = Vec::new();
            for resource in 0..resources.len() {
                let res = assign_task_on_resource(task, resource, est, &scheduled_tasks);
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
                            let res = assign_task_on_resource(task, resource, est, &scheduled_tasks);
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
