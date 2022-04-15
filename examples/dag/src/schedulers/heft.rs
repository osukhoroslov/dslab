use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::rc::Rc;

use network::model::NetworkModel;
use simcore::context::SimulationContext;
use simcore::{log_error, log_info, log_warn};

use dag::dag::DAG;
use dag::scheduler::{Action, Scheduler};
use dag::task::*;

#[derive(Clone, Debug)]
struct ScheduledTask {
    start_time: f64,
    end_time: f64,
}

impl ScheduledTask {
    fn new(start_time: f64, end_time: f64) -> ScheduledTask {
        ScheduledTask { start_time, end_time }
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
    network: Rc<RefCell<dyn NetworkModel>>,
}

impl HeftScheduler {
    pub fn new(network: Rc<RefCell<dyn NetworkModel>>) -> Self {
        HeftScheduler { network }
    }

    fn successors(v: usize, dag: &DAG) -> Vec<(usize, u64)> {
        let mut result = Vec::new();
        for &data_item_id in dag.get_task(v).outputs.iter() {
            let data_item = dag.get_data_item(data_item_id);
            result.extend(data_item.consumers.iter().map(|&v| (v, data_item.size * 2)));
        }
        result
    }

    fn calc_rank(
        &self,
        v: usize,
        avg_flop_time: f64,
        avg_netspeed: f64,
        dag: &DAG,
        rank: &mut Vec<f64>,
        used: &mut Vec<bool>,
    ) {
        if used[v] {
            return;
        }
        used[v] = true;

        rank[v] = 0.;
        for &(succ, edge_weight) in HeftScheduler::successors(v, dag).iter() {
            self.calc_rank(succ, avg_flop_time, avg_netspeed, dag, rank, used);
            rank[v] = rank[v].max(rank[succ] + edge_weight as f64 * avg_netspeed);
        }
        rank[v] += dag.get_task(v).flops as f64 * avg_flop_time;
    }
}

impl Scheduler for HeftScheduler {
    fn start(&mut self, dag: &DAG, resources: &Vec<dag::resource::Resource>, ctx: &SimulationContext) -> Vec<Action> {
        if dag.get_tasks().iter().any(|task| task.min_cores != task.max_cores) {
            log_warn!(
                ctx,
                "some tasks support different number of cores, but HEFT will always use min_cores"
            );
        }

        // average time over all resources for executing one flop
        let avg_flop_time = resources.iter().map(|r| 1. / r.speed as f64).sum::<f64>() / resources.len() as f64;

        let avg_netspeed = resources
            .iter()
            .map(|resource| 1. / self.network.borrow().bandwidth(ctx.id(), resource.id))
            .sum::<f64>()
            / resources.len() as f64;

        let total_tasks = dag.get_tasks().len();

        let mut used = vec![false; total_tasks];
        let mut rank = vec![0.; total_tasks];

        for i in 0..total_tasks {
            self.calc_rank(i, avg_flop_time, avg_netspeed, dag, &mut rank, &mut used);
        }

        let mut pred = vec![vec![(0 as usize, 0.); 0]; total_tasks];
        for task in 0..total_tasks {
            for &(succ, weight) in HeftScheduler::successors(task, dag).iter() {
                pred[succ].push((task, weight as f64));
            }
        }

        let mut tasks = (0..total_tasks).collect::<Vec<_>>();
        tasks.sort_by(|&a, &b| rank[b].partial_cmp(&rank[a]).unwrap());

        let mut scheduled_tasks = resources
            .iter()
            .map(|resource| {
                (0..resource.cores_available)
                    .map(|_| BTreeSet::<ScheduledTask>::new())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let mut eft = vec![0.; total_tasks];

        let mut result: Vec<(f64, Action)> = Vec::new();

        for task in tasks.into_iter() {
            let est = pred[task]
                .iter()
                .map(|&(task, weight)| eft[task] + weight * avg_netspeed)
                .max_by(|a, b| a.partial_cmp(&b).unwrap())
                .unwrap_or(0.);

            let mut best_finish = -1.;
            let mut best_time = -1.;
            let mut best_resource = 0 as usize;
            let mut best_cores: Vec<u32> = Vec::new();
            for resource in 0..resources.len() {
                let need_cores = dag.get_task(task).min_cores;
                if resources[resource].compute.borrow().cores_total() < need_cores {
                    continue;
                }

                let time = dag.get_task(task).flops as f64
                    / resources[resource].speed as f64
                    / dag.get_task(task).cores_dependency.speedup(need_cores);

                let mut possible_starts = scheduled_tasks[resource]
                    .iter()
                    .flat_map(|schedule| schedule.iter().map(|scheduled_task| scheduled_task.end_time))
                    .filter(|&a| a >= est)
                    .collect::<Vec<_>>();
                possible_starts.push(est);
                possible_starts.sort_by(|a, b| a.partial_cmp(&b).unwrap());
                possible_starts.dedup();

                let mut cores: Vec<u32> = Vec::new();
                let mut est = est;
                for &possible_start in possible_starts.iter() {
                    for core in 0..resources[resource].cores_available as usize {
                        let next = scheduled_tasks[resource][core]
                            .range((Excluded(ScheduledTask::new(possible_start, 0.)), Unbounded))
                            .next();
                        let prev = scheduled_tasks[resource][core]
                            .range((Unbounded, Included(ScheduledTask::new(possible_start, 0.))))
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
                if best_finish == -1. || best_finish > est + time {
                    best_time = time;
                    best_finish = est + time;
                    best_resource = resource;
                    best_cores = cores.iter().take(need_cores as usize).cloned().collect();
                }
            }

            if best_finish == -1. {
                log_error!(ctx, "couldn't schedule task {}, since every resource has less cores than minimum requirement for this task", dag.get_task(task).name);
                return Vec::new();
            }

            for &core in best_cores.iter() {
                scheduled_tasks[best_resource][core as usize]
                    .insert(ScheduledTask::new(best_finish - best_time, best_finish));
            }
            eft[task] = best_finish;
            result.push((
                best_finish - best_time,
                Action::ScheduleOnCores {
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
                .map(|cores| cores
                    .iter()
                    .map(|schedule| schedule.iter().next_back().map_or(0., |task| task.end_time))
                    .max_by(|a, b| a.partial_cmp(&b).unwrap())
                    .unwrap_or(0.))
                .max_by(|a, b| a.partial_cmp(&b).unwrap())
                .unwrap_or(0.)
        );

        result.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        result.into_iter().map(|(_, b)| b).collect()
    }

    fn on_task_state_changed(
        &mut self,
        _task: usize,
        _task_state: TaskState,
        _dag: &DAG,
        _resources: &Vec<dag::resource::Resource>,
        _ctx: &SimulationContext,
    ) -> Vec<Action> {
        Vec::new()
    }
}
