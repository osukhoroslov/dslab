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
    /// `total_comp_size / total_data_size`
    pub comp_data_ratio: f64,
    /// Longest path measured in sum of flops of tasks on this path.
    pub critical_path_size: f64,
    /// `total_comp_size / critical_path_size`
    pub parallelism_degree: f64,
    /// Number of levels.
    pub depth: usize,
    /// Size of the largest level.
    pub width: usize,
    /// Maximum number of tasks executed at the same time when executing
    /// on one resource with infinite number of cores.
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
        let mut std = ((sq_sum - 2. * sum * avg) / cnt as f64 + avg * avg).sqrt();
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
    /// Total number of tasks.
    pub task_count: usize,
    /// Stats about task size.
    pub task_size: SequenceStats,
    /// Stats about input size for one task.
    pub task_input_size: SequenceStats,
    /// Stats about output size for one task.
    pub task_output_size: SequenceStats,
    /// Total number of distinct predecessors.
    pub predecessor_count: usize,
    /// Total number of distinct successors.
    pub successor_count: usize,
    /// `predecessor_count / task_count`
    pub avg_predecessors: f64,
    /// `successor_count / task_count`
    pub avg_successors: f64,
}

impl DagStats {
    pub fn new(dag: &DAG) -> Self {
        let total_comp_size = dag.get_tasks().iter().map(|t| t.flops).sum();
        let total_data_size = dag.get_data_items().iter().map(|t| t.size).sum();
        let levels = Self::get_levels(dag);
        let critical_path_size = Self::get_critical_path_size(dag, &levels);
        DagStats {
            task_count: dag.get_tasks().len(),
            max_cores_per_task: dag.get_tasks().iter().map(|t| t.max_cores).max().unwrap(),
            total_comp_size,
            total_data_size,
            comp_data_ratio: total_comp_size / total_data_size,
            critical_path_size,
            parallelism_degree: total_comp_size / critical_path_size,
            depth: levels.len(),
            width: levels.iter().map(|l| l.len()).max().unwrap(),
            max_parallelism: Self::get_max_parallelism(dag, &levels),
            level_profiles: levels.iter().map(|l| Self::get_level_profile(dag, l)).collect(),
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
        for pred in dag.get_tasks()[task]
            .inputs
            .iter()
            .flat_map(|&data_item| dag.get_data_items()[data_item].producer)
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
                ranks[task] = dag.get_tasks()[task]
                    .outputs
                    .iter()
                    .flat_map(|&data_item| dag.get_data_items()[data_item].consumers.iter())
                    .map(|&succ| ranks[succ])
                    .max_by(|a, b| a.total_cmp(b))
                    .unwrap_or_default()
                    + dag.get_tasks()[task].flops;
            }
        }
        ranks.into_iter().max_by(|a, b| a.total_cmp(b)).unwrap()
    }

    fn get_max_parallelism(dag: &DAG, levels: &[Vec<usize>]) -> usize {
        let mut finish_times = vec![0f64; dag.get_tasks().len()];

        let mut events: Vec<(f64, isize)> = Vec::new();

        for tasks in levels.iter() {
            for &task in tasks.iter() {
                let start_time = dag.get_tasks()[task]
                    .inputs
                    .iter()
                    .filter_map(|&data_item| dag.get_data_items()[data_item].producer)
                    .map(|task| finish_times[task])
                    .max_by(|a, b| a.total_cmp(b))
                    .unwrap_or_default();
                let finish_time = start_time + dag.get_tasks()[task].flops;
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

    fn get_level_profile(dag: &DAG, level: &[usize]) -> LevelProfile {
        let predecessor_count = level
            .iter()
            .flat_map(|&t| dag.get_tasks()[t].inputs.iter().cloned())
            .filter(|data_item| !dag.get_inputs().contains(data_item))
            .collect::<HashSet<_>>()
            .len();
        let successor_count = level
            .iter()
            .flat_map(|&t| dag.get_tasks()[t].outputs.iter().cloned())
            .filter(|data_item| !dag.get_outputs().contains(data_item))
            .collect::<HashSet<_>>()
            .len();
        LevelProfile {
            task_count: level.len(),
            task_size: level.iter().map(|&t| dag.get_tasks()[t].flops).collect(),
            task_input_size: level
                .iter()
                .flat_map(|&t| dag.get_tasks()[t].inputs.iter())
                .map(|&data_item| dag.get_data_items()[data_item].size)
                .collect(),
            task_output_size: level
                .iter()
                .flat_map(|&t| dag.get_tasks()[t].outputs.iter())
                .map(|&data_item| dag.get_data_items()[data_item].size)
                .collect(),
            predecessor_count,
            successor_count,
            avg_predecessors: predecessor_count as f64 / level.len() as f64,
            avg_successors: successor_count as f64 / level.len() as f64,
        }
    }
}
