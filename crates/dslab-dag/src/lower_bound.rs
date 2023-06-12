use dslab_core::component::Id;

use crate::dag::DAG;
use crate::dag_stats::DagStats;
use crate::schedulers::common::task_successors;
use crate::system::System;

pub fn makespan_lower_bound(dag: &DAG, system: System, scheduler: Id) -> f64 {
    let stats = dag.stats();
    let max_bandwidth_from_scheduler = system
        .resources
        .iter()
        .map(|r| system.network.bandwidth(scheduler, r.id))
        .max_by(|a, b| a.total_cmp(b))
        .unwrap();
    let min_input_output_time = (stats.min_max_input_size + stats.min_max_output_size) / max_bandwidth_from_scheduler;
    [
        critical_path_time(dag, system) + min_input_output_time,
        total_comp_time(&stats, system) + min_input_output_time,
        input_time(&stats, max_bandwidth_from_scheduler),
        output_time(&stats, max_bandwidth_from_scheduler),
    ]
    .into_iter()
    .max_by(|a, b| a.total_cmp(b))
    .unwrap()
}

fn critical_path_time(dag: &DAG, system: System) -> f64 {
    let total_tasks = dag.get_tasks().len();
    let mut visited = vec![false; total_tasks];
    let mut ranks = vec![0.; total_tasks];
    for i in 0..total_tasks {
        calc_rank(i, system, dag, &mut ranks, &mut visited);
    }
    ranks.into_iter().max_by(|a, b| a.total_cmp(b)).unwrap_or_default()
}

fn total_comp_time(stats: &DagStats, system: System) -> f64 {
    stats.total_comp_size
        / system
            .resources
            .iter()
            .map(|r| r.speed * r.cores_available as f64)
            .sum::<f64>()
}

fn input_time(stats: &DagStats, max_bandwidth_from_scheduler: f64) -> f64 {
    stats.max_input_size / max_bandwidth_from_scheduler
}

fn output_time(stats: &DagStats, max_bandwidth_from_scheduler: f64) -> f64 {
    stats.max_output_size / max_bandwidth_from_scheduler
}

fn calc_rank(v: usize, system: System, dag: &DAG, ranks: &mut Vec<f64>, visited: &mut Vec<bool>) {
    if visited[v] {
        return;
    }
    visited[v] = true;
    ranks[v] = 0.;
    for &(succ, _edge_weight) in task_successors(v, dag).iter() {
        calc_rank(succ, system, dag, ranks, visited);
        ranks[v] = ranks[v].max(ranks[succ]);
    }
    let task = dag.get_task(v);
    ranks[v] += task.flops
        * system
            .resources
            .iter()
            .map(|r| 1. / r.speed / task.cores_dependency.speedup(r.cores_available.min(task.max_cores)))
            .min_by(|a, b| a.total_cmp(b))
            .unwrap();
}
