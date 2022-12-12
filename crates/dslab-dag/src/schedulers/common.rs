use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};
use std::ops::Bound::{Excluded, Included, Unbounded};

use dslab_core::context::SimulationContext;
use dslab_core::Id;
use dslab_network::network::Network;

use crate::dag::DAG;
use crate::data_item::{DataTransferMode, DataTransferStrategy};
use crate::runner::Config;

#[derive(Clone, Debug)]
pub struct ScheduledTask {
    pub start_time: f64,
    pub finish_time: f64,
    pub task: usize,
}

impl ScheduledTask {
    pub fn new(start_time: f64, finish_time: f64, task: usize) -> ScheduledTask {
        ScheduledTask {
            start_time,
            finish_time,
            task,
        }
    }
}

impl PartialOrd for ScheduledTask {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(
            self.start_time
                .total_cmp(&other.start_time)
                .then(self.finish_time.total_cmp(&other.finish_time)),
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
    task_id: usize,
    resource: usize,
    task_finish_times: &Vec<f64>,
    scheduled_tasks: &Vec<Vec<BTreeSet<ScheduledTask>>>,
    data_location: &HashMap<usize, Id>,
    task_location: &HashMap<usize, Id>,
    data_transfer_strategy: &DataTransferStrategy,
    dag: &DAG,
    resources: &Vec<crate::resource::Resource>,
    network: &Network,
    config: &Config,
    ctx: &SimulationContext,
) -> Option<(f64, f64, Vec<u32>)> {
    let need_cores = dag.get_task(task_id).min_cores;
    if resources[resource].compute.borrow().cores_total() < need_cores {
        return None;
    }

    let data_transfer_mode = &config.data_transfer_mode;

    let start_time = match data_transfer_strategy {
        DataTransferStrategy::Eager => dag
            .get_task(task_id)
            .inputs
            .iter()
            .map(|&id| dag.get_data_item(id))
            .filter(|&data_item| data_item.producer.is_some())
            .map(|data_item| (data_item.producer.unwrap(), data_item.size as f64))
            .map(|(task, weight)| {
                if task_location[&task] == resources[resource].id && data_transfer_mode == &DataTransferMode::Direct {
                    task_finish_times[task]
                } else {
                    task_finish_times[task]
                        + weight
                            * data_transfer_mode.net_time(
                                network,
                                task_location[&task],
                                resources[resource].id,
                                ctx.id(),
                            )
                }
            })
            .max_by(|a, b| a.total_cmp(&b))
            .unwrap_or(0.),
        DataTransferStrategy::Lazy => dag
            .get_task(task_id)
            .inputs
            .iter()
            .map(|&id| dag.get_data_item(id))
            .filter(|&data_item| data_item.producer.is_some())
            .map(|data_item| (data_item.producer.unwrap(), data_item.size as f64))
            .map(|(task, weight)| {
                let data_upload_time = match data_transfer_mode {
                    DataTransferMode::ViaMasterNode => weight / network.bandwidth(task_location[&task], ctx.id()),
                    DataTransferMode::Direct => 0.,
                    DataTransferMode::Manual => 0.,
                };
                task_finish_times[task] + data_upload_time
            })
            .max_by(|a, b| a.total_cmp(&b))
            .unwrap_or(0.),
    };

    let cur_net_time = 1. / network.bandwidth(ctx.id(), resources[resource].id);
    let input_load_time = match data_transfer_strategy {
        DataTransferStrategy::Eager => dag
            .get_task(task_id)
            .inputs
            .iter()
            .filter(|f| dag.get_inputs().contains(f))
            .map(|&f| dag.get_data_item(f).size as f64 * cur_net_time)
            .max_by(|a, b| a.total_cmp(&b))
            .unwrap_or(0.),
        DataTransferStrategy::Lazy => 0.,
    };
    let start_time = start_time.max(input_load_time);

    let download_time = match data_transfer_strategy {
        DataTransferStrategy::Eager => 0.,
        DataTransferStrategy::Lazy => dag
            .get_task(task_id)
            .inputs
            .iter()
            .map(|&f| match data_transfer_mode {
                DataTransferMode::ViaMasterNode => dag.get_data_item(f).size as f64 * cur_net_time,
                DataTransferMode::Direct => {
                    if !dag.get_inputs().contains(&f) {
                        if data_location[&f] == resources[resource].id {
                            0.
                        } else {
                            dag.get_data_item(f).size as f64
                                / network.bandwidth(data_location[&f], resources[resource].id)
                        }
                    } else {
                        dag.get_data_item(f).size as f64 * cur_net_time
                    }
                }
                DataTransferMode::Manual => 0.,
            })
            .max_by(|a, b| a.total_cmp(&b))
            .unwrap_or(0.),
    };
    let task_exec_time = dag.get_task(task_id).flops as f64
        / resources[resource].speed as f64
        / dag.get_task(task_id).cores_dependency.speedup(need_cores)
        + download_time;

    let (start_time, cores) = find_earliest_slot(&scheduled_tasks[resource], start_time, task_exec_time, need_cores);

    assert!(cores.len() >= need_cores as usize);

    let cores = cores.iter().take(need_cores as usize).cloned().collect::<Vec<_>>();

    Some((start_time, start_time + task_exec_time, cores))
}

fn find_earliest_slot(
    scheduled_tasks: &Vec<BTreeSet<ScheduledTask>>,
    mut start_time: f64,
    task_exec_time: f64,
    need_cores: u32,
) -> (f64, Vec<u32>) {
    let mut possible_starts = scheduled_tasks
        .iter()
        .flat_map(|schedule| schedule.iter().map(|scheduled_task| scheduled_task.finish_time))
        .filter(|&a| a >= start_time)
        .collect::<Vec<_>>();
    possible_starts.push(start_time);
    possible_starts.sort_by(|a, b| a.total_cmp(&b));
    possible_starts.dedup();

    let mut cores: Vec<u32> = Vec::new();
    for &possible_start in possible_starts.iter() {
        for core in 0..scheduled_tasks.len() {
            let next = scheduled_tasks[core]
                .range((Excluded(ScheduledTask::new(possible_start, 0., 0)), Unbounded))
                .next();
            let prev = scheduled_tasks[core]
                .range((Unbounded, Included(ScheduledTask::new(possible_start, 0., 0))))
                .next_back();
            if let Some(scheduled_task) = prev {
                if scheduled_task.finish_time > possible_start {
                    continue;
                }
            }
            if let Some(scheduled_task) = next {
                if scheduled_task.start_time < possible_start + task_exec_time {
                    continue;
                }
            }
            cores.push(core as u32);
        }
        if cores.len() >= need_cores as usize {
            start_time = possible_start;
            break;
        } else {
            cores.clear();
        }
    }
    (start_time, cores)
}

pub fn task_successors(v: usize, dag: &DAG) -> Vec<(usize, u64)> {
    let mut result = Vec::new();
    for &data_item_id in dag.get_task(v).outputs.iter() {
        let data_item = dag.get_data_item(data_item_id);
        result.extend(data_item.consumers.iter().map(|&v| (v, data_item.size)));
    }
    result
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

fn topsort_dfs(v: usize, dag: &DAG, used: &mut Vec<bool>, order: &mut Vec<usize>) {
    used[v] = true;
    for &(succ, _) in task_successors(v, dag).iter() {
        if !used[succ] {
            topsort_dfs(succ, dag, used, order);
        }
    }
    order.push(v);
}

pub fn topsort(dag: &DAG) -> Vec<usize> {
    let mut order = Vec::with_capacity(dag.get_tasks().len());
    let mut used = vec![false; dag.get_tasks().len()];
    for i in 0..dag.get_tasks().len() {
        if !used[i] {
            topsort_dfs(i, dag, &mut used, &mut order);
        }
    }
    assert_eq!(order.len(), dag.get_tasks().len());
    order.reverse();
    order
}
