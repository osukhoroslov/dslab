use std::cmp::Ordering;
use std::collections::{BTreeSet, HashSet};
use std::ops::Bound::{Excluded, Included, Unbounded};

use dslab_core::context::SimulationContext;
use dslab_network::network::Network;

use crate::dag::DAG;
use crate::data_item::{DataTransferMode, DataTransferStrategy};
use crate::runner::Config;

#[derive(Clone, Debug)]
pub struct ScheduledTask {
    pub start_time: f64,
    pub end_time: f64,
    pub task: usize,
}

impl ScheduledTask {
    pub fn new(start_time: f64, end_time: f64, task: usize) -> ScheduledTask {
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

pub fn evaluate_assignment(
    task: usize,
    resource: usize,
    est: f64,
    scheduled_tasks: &Vec<Vec<BTreeSet<ScheduledTask>>>,
    outputs: &HashSet<usize>,
    avg_net_time: f64,
    data_transfer_strategy: &DataTransferStrategy,
    dag: &DAG,
    resources: &Vec<crate::resource::Resource>,
    network: &Network,
    config: &Config,
    ctx: &SimulationContext,
) -> Option<(f64, f64, Vec<u32>)> {
    let data_transfer_mode = &config.data_transfer_mode;

    let cur_net_time = 1. / network.bandwidth(ctx.id(), resources[resource].id);
    let input_load_time = match data_transfer_strategy {
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

    let download_time = match data_transfer_strategy {
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
}

pub fn task_successors(v: usize, dag: &DAG) -> Vec<(usize, u64)> {
    let mut result = Vec::new();
    for &data_item_id in dag.get_task(v).outputs.iter() {
        let data_item = dag.get_data_item(data_item_id);
        result.extend(data_item.consumers.iter().map(|&v| (v, data_item.size)));
    }
    result
}

pub fn predecessors(dag: &DAG) -> Vec<Vec<(usize, f64)>> {
    let total_tasks = dag.get_tasks().len();

    let mut predecessors = vec![vec![(0 as usize, 0.); 0]; total_tasks];
    for task in 0..total_tasks {
        for &(succ, weight) in task_successors(task, dag).iter() {
            predecessors[succ].push((task, weight as f64));
        }
    }
    predecessors
}

fn calc_rank(v: usize, avg_flop_time: f64, avg_net_time: f64, dag: &DAG, ranks: &mut Vec<f64>, used: &mut Vec<bool>) {
    if used[v] {
        return;
    }
    used[v] = true;

    ranks[v] = 0.;
    for &(succ, edge_weight) in task_successors(v, dag).iter() {
        calc_rank(succ, avg_flop_time, avg_net_time, dag, ranks, used);
        ranks[v] = ranks[v].max(ranks[succ] + edge_weight as f64 * avg_net_time);
    }
    ranks[v] += dag.get_task(v).flops as f64 * avg_flop_time;
}

pub fn calc_ranks(avg_flop_time: f64, avg_net_time: f64, dag: &DAG) -> Vec<f64> {
    let total_tasks = dag.get_tasks().len();

    let mut used = vec![false; total_tasks];
    let mut ranks = vec![0.; total_tasks];

    for i in 0..total_tasks {
        calc_rank(i, avg_flop_time, avg_net_time, dag, &mut ranks, &mut used);
    }

    ranks
}
