use std::collections::HashSet;

use serde::Serialize;

use crate::dag::DAG;

#[derive(Debug, Clone, Serialize)]
pub struct DagStats {
    /// Total number of tasks.
    pub task_count: usize,
    /// Max of max_cores among all tasks.
    pub max_cores_per_task: u32,
    /// Sum of flops of all tasks.
    pub total_comp_size: f64,
    /// Sum of sizes of all data items.
    pub total_data_size: f64,
    /// Sum of sizes of all data transfers
    /// (differs from `total_data_size` if some data items are read by multiple tasks).
    pub total_transfers_size: f64,
    /// Sum of sizes of all DAG input data items
    /// (i.e. data items not produced by the tasks).
    pub input_data_size: f64,
    /// Sum of sizes of all DAG output data items
    /// (i.e. data items not consumed by the tasks).
    pub output_data_size: f64,
    /// Maximum size of DAG input data item.
    pub max_input_size: f64,
    /// Maximum size of DAG output data item.
    pub max_output_size: f64,
    /// Minimum of maximum task input size among all entry tasks (first level).
    pub min_max_input_size: f64,
    /// Minimum of maximum task output size among all exit tasks (last level).
    pub min_max_output_size: f64,
    /// Computation-to-communication ratio computed as `total_comp_size / total_transfers_size`.
    pub comp_transfers_ratio: f64,
    /// Longest path in the DAG measured in sum of flops of tasks on this path.
    pub critical_path_size: f64,
    /// Parallelism degree computed as `total_comp_size / critical_path_size`.
    pub parallelism_degree: f64,
    /// Number of levels.
    pub depth: usize,
    /// Size of the largest level (number of tasks).
    pub width: usize,
    /// Maximum number of tasks that can be executed in parallel
    /// (obtained by running the DAG on a single resource with infinite number of cores).
    pub max_parallelism: usize,
    /// Stats for each level.
    pub level_profiles: Vec<LevelProfile>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SequenceStats {
    pub min: f64,
    pub max: f64,
    pub sum: f64,
    pub avg: f64,
    /// Standard deviation.
    pub std: f64,
}

impl FromIterator<f64> for SequenceStats {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = f64>,
    {
        let mut min = f64::MAX;
        let mut max = f64::MIN;
        let mut sum = 0.;
        let mut sq_sum = 0.;
        let mut cnt = 0usize;
        let mut v = Vec::new();
        for val in iter {
            v.push(val);
            min = min.min(val);
            max = max.max(val);
            sum += val;
            sq_sum += val * val;
            cnt += 1;
        }

        let mut avg = sum / cnt as f64;
        let mut std = (((sq_sum - 2. * sum * avg) / cnt as f64 + avg * avg).max(0.)).sqrt();
        if cnt == 0 {
            min = 0.;
            max = 0.;
            avg = 0.;
            std = 0.;
        }
        Self {
            min,
            max,
            sum,
            avg,
            std,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct LevelProfile {
    /// Level index.
    pub level: usize,
    /// Total number of tasks.
    pub task_count: usize,
    /// Stats about task sizes.
    pub task_comp_size: SequenceStats,
    /// Stats about task input sizes.
    pub task_input_size: SequenceStats,
    /// Stats about task output sizes.
    pub task_output_size: SequenceStats,
    /// Total number of distinct predecessors.
    pub predecessor_count: usize,
    /// Total number of distinct successors.
    pub successor_count: usize,
    /// Average number of task predecessors.
    pub avg_predecessors: f64,
    /// Average number of task successors.
    pub avg_successors: f64,
}

impl DagStats {
    pub fn new(dag: &DAG) -> Self {
        let total_comp_size = dag.get_tasks().iter().map(|t| t.flops).sum();
        let total_data_size = dag.get_data_items().iter().map(|t| t.size).sum();
        let total_transfers_size = dag
            .get_data_items()
            .iter()
            .map(|t| t.size * t.consumers.len().max(1) as f64)
            .sum();
        let levels = Self::get_levels(dag);
        let critical_path_size = Self::get_critical_path_size(dag, &levels);
        DagStats {
            task_count: dag.get_tasks().len(),
            max_cores_per_task: dag.get_tasks().iter().map(|t| t.max_cores).max().unwrap(),
            total_comp_size,
            total_data_size,
            total_transfers_size,
            input_data_size: dag.get_inputs().iter().map(|&i| dag.get_data_item(i).size).sum(),
            output_data_size: dag.get_outputs().iter().map(|&i| dag.get_data_item(i).size).sum(),
            max_input_size: dag
                .get_inputs()
                .iter()
                .map(|&i| dag.get_data_item(i).size)
                .max_by(|a, b| a.total_cmp(b))
                .unwrap_or_default(),
            max_output_size: dag
                .get_outputs()
                .iter()
                .map(|&i| dag.get_data_item(i).size)
                .max_by(|a, b| a.total_cmp(b))
                .unwrap_or_default(),
            min_max_input_size: dag
                .get_tasks()
                .iter()
                .filter_map(|t| {
                    t.inputs
                        .iter()
                        .filter(|&data_item| dag.get_inputs().contains(data_item))
                        .map(|&data_item| dag.get_data_item(data_item).size)
                        .max_by(|a, b| a.total_cmp(b))
                })
                .min_by(|a, b| a.total_cmp(b))
                .unwrap_or_default(),
            min_max_output_size: dag
                .get_tasks()
                .iter()
                .filter_map(|t| {
                    t.outputs
                        .iter()
                        .filter(|&data_item| dag.get_outputs().contains(data_item))
                        .map(|&data_item| dag.get_data_item(data_item).size)
                        .max_by(|a, b| a.total_cmp(b))
                })
                .min_by(|a, b| a.total_cmp(b))
                .unwrap_or_default(),
            comp_transfers_ratio: total_comp_size / total_transfers_size,
            critical_path_size,
            parallelism_degree: total_comp_size / critical_path_size,
            depth: levels.len(),
            width: levels.iter().map(|l| l.len()).max().unwrap(),
            max_parallelism: Self::get_max_parallelism(dag, &levels),
            level_profiles: levels
                .iter()
                .enumerate()
                .map(|(i, l)| Self::get_level_profile(dag, i, l))
                .collect(),
        }
    }

    fn get_levels(dag: &DAG) -> Vec<Vec<usize>> {
        let mut levels = vec![usize::MAX; dag.get_tasks().len()];
        for task in 0..dag.get_tasks().len() {
            Self::dfs(dag, task, &mut levels);
        }
        let mut result = vec![Vec::new(); levels.iter().max().unwrap() + 1];
        for task in 0..dag.get_tasks().len() {
            result[levels[task]].push(task);
        }
        result
    }

    fn dfs(dag: &DAG, task: usize, levels: &mut [usize]) {
        if levels[task] != usize::MAX {
            return;
        }
        let mut level = 0;
        for pred in dag
            .get_task(task)
            .inputs
            .iter()
            .flat_map(|&data_item| dag.get_data_item(data_item).producer)
        {
            Self::dfs(dag, pred, levels);
            level = level.max(levels[pred] + 1);
        }
        levels[task] = level;
    }

    fn get_critical_path_size(dag: &DAG, levels: &[Vec<usize>]) -> f64 {
        let mut ranks = vec![0f64; dag.get_tasks().len()];
        for tasks in levels.iter().rev() {
            for &task in tasks.iter() {
                ranks[task] = dag
                    .get_task(task)
                    .outputs
                    .iter()
                    .flat_map(|&data_item| dag.get_data_item(data_item).consumers.iter())
                    .map(|&succ| ranks[succ])
                    .max_by(|a, b| a.total_cmp(b))
                    .unwrap_or_default()
                    + dag.get_task(task).flops;
            }
        }
        ranks.into_iter().max_by(|a, b| a.total_cmp(b)).unwrap()
    }

    fn get_max_parallelism(dag: &DAG, levels: &[Vec<usize>]) -> usize {
        let mut finish_times = vec![0f64; dag.get_tasks().len()];

        let mut events: Vec<(f64, isize)> = Vec::new();

        for tasks in levels.iter() {
            for &task in tasks.iter() {
                let start_time = dag
                    .get_task(task)
                    .inputs
                    .iter()
                    .filter_map(|&data_item| dag.get_data_item(data_item).producer)
                    .map(|task| finish_times[task])
                    .max_by(|a, b| a.total_cmp(b))
                    .unwrap_or_default();
                let finish_time = start_time + dag.get_task(task).flops;
                events.push((start_time, 1));
                events.push((finish_time, -1));
                finish_times[task] = finish_time;
            }
        }

        events.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        let mut cur = 0;
        let mut max_parallelism = 0;
        for (_, delta) in events.into_iter() {
            cur += delta;
            max_parallelism = max_parallelism.max(cur);
        }
        max_parallelism as usize
    }

    fn get_level_profile(dag: &DAG, level_index: usize, level: &[usize]) -> LevelProfile {
        let predecessor_count = level
            .iter()
            .flat_map(|&t| dag.get_task(t).inputs.iter().cloned())
            .filter_map(|data_item| dag.get_data_item(data_item).producer)
            .collect::<HashSet<usize>>()
            .len();
        let successor_count = level
            .iter()
            .flat_map(|&t| dag.get_task(t).outputs.iter().cloned())
            .flat_map(|data_item| dag.get_data_item(data_item).consumers.iter().cloned())
            .collect::<HashSet<usize>>()
            .len();
        LevelProfile {
            level: level_index,
            task_count: level.len(),
            task_comp_size: level.iter().map(|&t| dag.get_task(t).flops).collect(),
            task_input_size: level
                .iter()
                .flat_map(|&t| dag.get_task(t).inputs.iter())
                .map(|&data_item| dag.get_data_item(data_item).size)
                .collect(),
            task_output_size: level
                .iter()
                .flat_map(|&t| dag.get_task(t).outputs.iter())
                .map(|&data_item| dag.get_data_item(data_item).size)
                .collect(),
            predecessor_count,
            successor_count,
            avg_predecessors: level
                .iter()
                .map(|&t| {
                    dag.get_task(t)
                        .inputs
                        .iter()
                        .filter_map(|&data_item| dag.get_data_item(data_item).producer)
                        .collect::<HashSet<usize>>()
                        .len() as f64
                })
                .sum::<f64>()
                / level.len() as f64,
            avg_successors: level
                .iter()
                .map(|&t| {
                    dag.get_task(t)
                        .outputs
                        .iter()
                        .flat_map(|&data_item| dag.get_data_item(data_item).consumers.iter().copied())
                        .collect::<HashSet<usize>>()
                        .len() as f64
                })
                .sum::<f64>()
                / level.len() as f64,
        }
    }
}
