use dslab_core::component::Id;

use crate::dag::DAG;
use crate::dag_stats::DagStats;
use crate::schedulers::common::task_successors;
use crate::system::System;

pub fn makespan_lower_bound(dag: &DAG, system: System, _scheduler: Id) -> f64 {
    critical_path_time(dag, system).max(total_comp_time(&dag.stats(), system))
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
