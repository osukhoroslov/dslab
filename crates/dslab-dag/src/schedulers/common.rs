use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};
use std::ops::Bound::{Excluded, Included, Unbounded};

use dslab_core::context::SimulationContext;
use dslab_core::Id;
use dslab_network::Network;

use crate::dag::DAG;
use crate::data_item::{DataTransferMode, DataTransferStrategy};
use crate::runner::Config;
use crate::schedulers::treap::Treap;

#[derive(Clone, Debug, PartialEq)]
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
                .then(self.finish_time.total_cmp(&other.finish_time))
                .then(self.task.cmp(&other.task)),
        )
    }
}

impl Ord for ScheduledTask {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl Eq for ScheduledTask {}

#[allow(clippy::too_many_arguments)]
pub fn evaluate_assignment(
    task_id: usize,
    resource: usize,
    task_finish_times: &[f64],
    scheduled_tasks: &[Vec<BTreeSet<ScheduledTask>>],
    memory_usage: &[Treap],
    data_location: &HashMap<usize, Id>,
    task_location: &HashMap<usize, Id>,
    data_transfer_strategy: &DataTransferStrategy,
    dag: &DAG,
    resources: &[crate::resource::Resource],
    network: &Network,
    config: &Config,
    ctx: &SimulationContext,
) -> Option<(f64, f64, Vec<u32>)> {
    let need_cores = dag.get_task(task_id).min_cores;
    if resources[resource].compute.borrow().cores_total() < need_cores {
        return None;
    }
    let need_memory = dag.get_task(task_id).memory;
    if resources[resource].compute.borrow().memory_total() < need_memory {
        return None;
    }
    if !dag.get_task(task_id).is_allowed_on(resource) {
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
            .map(|data_item| (data_item.producer.unwrap(), data_item.size))
            .map(|(task, weight)| match task_location.get(&task) {
                Some(location) => {
                    if *location == resources[resource].id && data_transfer_mode == &DataTransferMode::Direct {
                        task_finish_times[task]
                    } else {
                        task_finish_times[task]
                            + weight * data_transfer_mode.net_time(network, *location, resources[resource].id, ctx.id())
                    }
                }
                // If the task has unscheduled parents (e.g. evaluation of children tasks in Lookahead),
                // we ignore delays due to these parents, which makes start_time rather optimistic.
                None => 0.,
            })
            .max_by(|a, b| a.total_cmp(b))
            .unwrap_or(0.),
        DataTransferStrategy::Lazy => dag
            .get_task(task_id)
            .inputs
            .iter()
            .map(|&id| dag.get_data_item(id))
            .filter(|&data_item| data_item.producer.is_some())
            .map(|data_item| (data_item.producer.unwrap(), data_item.size))
            .map(|(task, weight)| {
                let data_upload_time = match data_transfer_mode {
                    DataTransferMode::ViaMasterNode => {
                        network.latency(task_location[&task], ctx.id())
                            + weight / network.bandwidth(task_location[&task], ctx.id())
                    }
                    DataTransferMode::Direct => 0.,
                    DataTransferMode::Manual => 0.,
                };
                task_finish_times[task] + data_upload_time
            })
            .max_by(|a, b| a.total_cmp(b))
            .unwrap_or(0.),
    };

    let download_time = match data_transfer_strategy {
        DataTransferStrategy::Eager => 0.,
        DataTransferStrategy::Lazy => dag
            .get_task(task_id)
            .inputs
            .iter()
            .map(|&f| match data_transfer_mode {
                DataTransferMode::ViaMasterNode => {
                    network.latency(ctx.id(), resources[resource].id)
                        + dag.get_data_item(f).size / network.bandwidth(ctx.id(), resources[resource].id)
                }
                DataTransferMode::Direct => {
                    if data_location[&f] == resources[resource].id {
                        0.
                    } else {
                        network.latency(data_location[&f], resources[resource].id)
                            + dag.get_data_item(f).size / network.bandwidth(data_location[&f], resources[resource].id)
                    }
                }
                DataTransferMode::Manual => 0.,
            })
            .max_by(|a, b| a.total_cmp(b))
            .unwrap_or(0.),
    };
    let task_exec_time = dag.get_task(task_id).flops
        / resources[resource].speed
        / dag.get_task(task_id).cores_dependency.speedup(need_cores)
        + download_time;

    let (start_time, cores) = find_earliest_slot(
        &scheduled_tasks[resource],
        start_time,
        task_exec_time,
        need_cores,
        need_memory,
        resources[resource].compute.borrow().memory_total(),
        &memory_usage[resource],
    );

    assert!(cores.len() >= need_cores as usize);

    let cores = cores.iter().take(need_cores as usize).cloned().collect::<Vec<_>>();

    Some((start_time, start_time + task_exec_time, cores))
}

fn find_earliest_slot(
    scheduled_tasks: &[BTreeSet<ScheduledTask>],
    mut start_time: f64,
    task_exec_time: f64,
    need_cores: u32,
    need_memory: u64,
    total_memory: u64,
    memory_usage: &Treap,
) -> (f64, Vec<u32>) {
    // current iterators to a position in BTreeSet where ScheduledTask::new(possible_start, 0., 0) is supposed to be
    let mut iters = scheduled_tasks
        .iter()
        .map(|tasks| {
            tasks
                .range((Unbounded, Included(ScheduledTask::new(start_time, 0., 0))))
                .rev()
                .take(1)
                .chain(tasks.range((Excluded(ScheduledTask::new(start_time, 0., 0)), Unbounded)))
                .peekable()
        })
        .collect::<Vec<_>>();
    // last item to the left of corresponding iter
    let mut last_task: Vec<Option<&ScheduledTask>> = vec![None; scheduled_tasks.len()];

    let mut cores: Vec<u32> = Vec::new();

    let mut possible_start = start_time;
    loop {
        for core in 0..scheduled_tasks.len() {
            while let Some(&task) = iters[core].peek() {
                if task.start_time <= possible_start {
                    last_task[core] = iters[core].next();
                } else {
                    break;
                }
            }
        }

        if memory_usage.max(possible_start, possible_start + task_exec_time) + need_memory <= total_memory {
            for core in 0..scheduled_tasks.len() {
                let next = iters[core].peek();
                let prev = last_task[core];
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

        let next_possible_start = last_task
            .iter()
            .filter_map(|task| task.map(|t| t.finish_time))
            .chain(
                iters
                    .iter_mut()
                    .filter_map(|iter| iter.peek().map(|task| task.finish_time)),
            )
            .filter(|time| time > &possible_start)
            .min_by(|a, b| a.total_cmp(b));
        if let Some(time) = next_possible_start {
            assert!(time > possible_start);
            possible_start = time;
        } else {
            break;
        }
    }

    (start_time, cores)
}

pub fn task_successors(v: usize, dag: &DAG) -> Vec<(usize, f64)> {
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
        ranks[v] = ranks[v].max(ranks[succ] + edge_weight * avg_net_time);
    }
    ranks[v] += dag.get_task(v).flops * avg_flop_time;
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
