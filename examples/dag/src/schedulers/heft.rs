use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::ops::Bound::{Excluded, Included, Unbounded};

use simcore::context::SimulationContext;
use simcore::{log_debug, log_error, log_info};

use dag::dag::DAG;
use dag::scheduler::{Action, Scheduler};
use dag::task::*;

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
        self.start_time.partial_cmp(&other.start_time)
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

pub struct HeftScheduler {
    bandwidth: f64,
    scheduled_tasks: Vec<Vec<BTreeSet<ScheduledTask>>>,
}

impl HeftScheduler {
    pub fn new(bandwidth: f64) -> Self {
        HeftScheduler {
            bandwidth,
            scheduled_tasks: Vec::new(),
        }
    }

    fn successors(v: usize, dag: &DAG) -> Vec<(usize, u64)> {
        let mut result = Vec::new();
        for &data_item_id in dag.get_task(v).outputs.iter() {
            let data_item = dag.get_data_item(data_item_id);
            result.extend(data_item.consumers.iter().map(|&v| (v, data_item.size * 2)));
        }
        result
    }

    fn calc_rank(&self, v: usize, avg_flop_time: f64, dag: &DAG, rank: &mut Vec<f64>, used: &mut Vec<bool>) {
        if used[v] {
            return;
        }

        rank[v] = 0.;
        for &(succ, edge_weight) in HeftScheduler::successors(v, dag).iter() {
            self.calc_rank(succ, avg_flop_time, dag, rank, used);
            rank[v] = rank[v].max(rank[succ] + edge_weight as f64 / self.bandwidth);
        }
        rank[v] += dag.get_task(v).flops as f64 * avg_flop_time;
    }

    fn schedule(&mut self, dag: &DAG, ctx: &SimulationContext) -> Vec<Action> {
        let mut result = Vec::new();

        for resource in 0..self.scheduled_tasks.len() {
            for core in 0..self.scheduled_tasks[resource].len() {
                let schedule = &mut self.scheduled_tasks[resource][core];
                while !schedule.is_empty()
                    && dag.get_task(schedule.iter().next().unwrap().task).state == TaskState::Done
                {
                    let item = schedule.iter().next().unwrap().clone();
                    schedule.remove(&item);
                }

                if !schedule.is_empty() && dag.get_task(schedule.iter().next().unwrap().task).state == TaskState::Ready
                {
                    let item = schedule.iter().next().unwrap();
                    result.push(Action::Schedule {
                        task: item.task,
                        resource: resource,
                        cores: 1,
                    });
                    log_debug!(
                        ctx,
                        "task {} scheduled now, planned: {:.3}-{:.3}",
                        dag.get_task(item.task).name,
                        item.start_time,
                        item.end_time,
                    );
                }
            }
        }

        result
    }
}

impl Scheduler for HeftScheduler {
    fn start(&mut self, dag: &DAG, resources: &Vec<dag::resource::Resource>, ctx: &SimulationContext) -> Vec<Action> {
        if dag
            .get_tasks()
            .iter()
            .any(|task| task.min_cores > 1 || task.max_cores < 1)
        {
            log_error!(
                ctx,
                "HEFT can only run tasks on one core and some input tasks don't support it"
            );
            return Vec::new();
        }

        // average time over all resources for executing one flop
        let avg_flop_time = resources.iter().map(|r| 1. / r.speed as f64).sum::<f64>() / resources.len() as f64;

        let total_tasks = dag.get_tasks().len();

        let mut used = vec![false; total_tasks];
        let mut rank = vec![0.; total_tasks];

        for i in 0..total_tasks {
            self.calc_rank(i, avg_flop_time, dag, &mut rank, &mut used);
        }

        let mut pred = vec![vec![(0 as usize, 0.); 0]; total_tasks];
        for task in 0..total_tasks {
            for &(succ, weight) in HeftScheduler::successors(task, dag).iter() {
                pred[succ].push((task, weight as f64));
            }
        }

        let mut tasks = (0..total_tasks).collect::<Vec<_>>();
        tasks.sort_by(|&a, &b| rank[b].partial_cmp(&rank[a]).unwrap());

        self.scheduled_tasks = resources
            .iter()
            .map(|resource| {
                (0..resource.cores_available)
                    .map(|_| BTreeSet::<ScheduledTask>::new())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let mut eft = vec![0.; total_tasks];

        for task in tasks.into_iter() {
            let est = pred[task]
                .iter()
                .map(|&(task, weight)| eft[task] + weight / self.bandwidth)
                .max_by(|a, b| a.partial_cmp(&b).unwrap())
                .unwrap_or(0.);

            let mut best_finish = -1.;
            let mut best_resource = 0 as usize;
            let mut best_core = 0 as usize;
            for resource in 0..resources.len() {
                let time = dag.get_task(task).flops as f64 / resources[resource].speed as f64;
                for core in 0..resources[resource].cores_available as usize {
                    let mut est = est;
                    let range = self.scheduled_tasks[resource][core]
                        .range((Excluded(ScheduledTask::new(est, 0., 0 as usize)), Unbounded));
                    let prev = self.scheduled_tasks[resource][core]
                        .range((Unbounded, Included(ScheduledTask::new(est, 0., 0 as usize))))
                        .next_back();
                    if let Some(scheduled_task) = prev {
                        est = est.max(scheduled_task.end_time);
                    }
                    for next in range {
                        if next.start_time >= est + time {
                            break;
                        }
                        est = est.max(next.end_time);
                    }
                    if best_finish == -1. || best_finish > est + time {
                        best_finish = est + time;
                        best_resource = resource;
                        best_core = core;
                    }
                }
            }

            self.scheduled_tasks[best_resource][best_core].insert(ScheduledTask::new(
                best_finish - dag.get_task(task).flops as f64 / resources[best_resource].speed as f64,
                best_finish,
                task,
            ));
            eft[task] = best_finish;
        }

        log_info!(
            ctx,
            "expected timespan: {:.3}",
            self.scheduled_tasks
                .iter()
                .map(|cores| cores
                    .iter()
                    .map(|schedule| schedule.iter().next_back().map_or(0., |task| task.end_time))
                    .max_by(|a, b| a.partial_cmp(&b).unwrap())
                    .unwrap_or(0.))
                .max_by(|a, b| a.partial_cmp(&b).unwrap())
                .unwrap_or(0.)
        );

        self.schedule(dag, ctx)
    }

    fn on_task_state_changed(
        &mut self,
        _task: usize,
        _task_state: TaskState,
        dag: &DAG,
        _resources: &Vec<dag::resource::Resource>,
        ctx: &SimulationContext,
    ) -> Vec<Action> {
        self.schedule(dag, ctx)
    }
}
