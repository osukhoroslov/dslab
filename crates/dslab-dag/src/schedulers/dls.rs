use std::collections::{BTreeSet, HashMap};

use dslab_core::context::SimulationContext;
use dslab_core::log_warn;
use dslab_core::Id;

use crate::dag::DAG;
use crate::data_item::{DataTransferMode, DataTransferStrategy};
use crate::runner::Config;
use crate::scheduler::{Action, Scheduler, SchedulerParams, TimeSpan};
use crate::schedulers::common::*;
use crate::schedulers::treap::Treap;
use crate::system::System;

pub struct DlsScheduler {
    data_transfer_strategy: DataTransferStrategy,
}

impl DlsScheduler {
    pub fn new() -> Self {
        Self {
            data_transfer_strategy: DataTransferStrategy::Eager,
        }
    }

    pub fn from_params(params: &SchedulerParams) -> Self {
        Self {
            data_transfer_strategy: params
                .get("data_transfer_strategy")
                .unwrap_or(DataTransferStrategy::Eager),
        }
    }

    pub fn with_data_transfer_strategy(mut self, data_transfer_strategy: DataTransferStrategy) -> Self {
        self.data_transfer_strategy = data_transfer_strategy;
        self
    }

    fn schedule(&self, dag: &DAG, system: System, config: Config, ctx: &SimulationContext) -> Vec<Action> {
        let resources = system.resources;
        let network = system.network;

        let avg_net_time = system.avg_net_time(ctx.id(), &config.data_transfer_mode);

        let task_count = dag.get_tasks().len();

        let median_flop_time = median(
            system
                .resources
                .iter()
                .filter(|r| r.name != "master")
                .map(|r| 1. / r.speed),
        );

        let task_static_levels = calc_ranks(median_flop_time, avg_net_time, dag);
        let mut task_ids = (0..task_count).collect::<Vec<_>>();
        task_ids.sort_by(|&a, &b| task_static_levels[b].total_cmp(&task_static_levels[a]));

        let mut scheduled = vec![false; task_count];
        let mut task_finish_times = vec![0.; task_count];
        let mut scheduled_tasks: Vec<Vec<BTreeSet<ScheduledTask>>> = resources
            .iter()
            .map(|resource| (0..resource.cores_available).map(|_| BTreeSet::new()).collect())
            .collect();
        let mut memory_usage: Vec<Treap> = (0..resources.len()).map(|_| Treap::new()).collect();
        let mut data_locations: HashMap<usize, Id> = HashMap::new();
        let mut task_locations: HashMap<usize, Id> = HashMap::new();

        let mut result: Vec<(f64, Action)> = Vec::new();

        for _ in 0..task_ids.len() {
            // stores (task_id, resource) pair with the best dynamic level value
            let mut best_pair: Option<(usize, usize)> = None;
            let mut best_dl: f64 = f64::MIN;
            let mut best_start = -1.;
            let mut best_finish = -1.;
            let mut best_cores: Vec<u32> = Vec::new();
            for &task_id in task_ids.iter().filter(|&i| !scheduled[*i]).filter(|&i| {
                dag.get_task(*i)
                    .inputs
                    .iter()
                    .filter_map(|&id| dag.get_data_item(id).producer)
                    .all(|task| scheduled[task])
            }) {
                for resource in 0..resources.len() {
                    let res = evaluate_assignment(
                        task_id,
                        resource,
                        &task_finish_times,
                        &scheduled_tasks,
                        &memory_usage,
                        &data_locations,
                        &task_locations,
                        &self.data_transfer_strategy,
                        dag,
                        resources,
                        network,
                        &config,
                        ctx,
                    );
                    if res.is_none() {
                        continue;
                    }
                    let (start_time, finish_time, cores) = res.unwrap();

                    let delta =
                        dag.get_task(task_id).flops * (median_flop_time - 1. / system.resources[resource].speed);
                    let current_score = task_static_levels[task_id] - start_time + delta;
                    if current_score > best_dl {
                        best_dl = current_score;
                        best_pair = Some((task_id, resource));
                        best_start = start_time;
                        best_finish = finish_time;
                        best_cores = cores;
                    }
                }
            }

            let (task_id, resource) = best_pair.unwrap();

            scheduled[task_id] = true;
            task_finish_times[task_id] = best_finish;
            for &core in best_cores.iter() {
                scheduled_tasks[resource][core as usize].insert(ScheduledTask::new(best_start, best_finish, task_id));
            }
            memory_usage[resource].add(best_start, best_finish, dag.get_task(task_id).memory);
            for &output in dag.get_task(task_id).outputs.iter() {
                data_locations.insert(output, resources[resource].id);
            }
            task_locations.insert(task_id, resources[resource].id);

            result.push((
                best_start,
                Action::ScheduleTaskOnCores {
                    task: task_id,
                    resource,
                    cores: best_cores,
                    expected_span: Some(TimeSpan::new(best_start, best_finish)),
                },
            ));
        }

        result.sort_by(|a, b| a.0.total_cmp(&b.0));
        result.into_iter().map(|(_, b)| b).collect()
    }
}

impl Scheduler for DlsScheduler {
    fn start(&mut self, dag: &DAG, system: System, config: Config, ctx: &SimulationContext) -> Vec<Action> {
        assert_ne!(
            config.data_transfer_mode,
            DataTransferMode::Manual,
            "DlsScheduler doesn't support DataTransferMode::Manual"
        );

        if dag.get_tasks().iter().any(|task| task.min_cores != task.max_cores) {
            log_warn!(
                ctx,
                "some tasks support different number of cores, but DLS will always use min_cores"
            );
        }

        self.schedule(dag, system, config, ctx)
    }

    fn is_static(&self) -> bool {
        true
    }
}

impl Default for DlsScheduler {
    fn default() -> Self {
        Self::new()
    }
}

fn median(data: impl Iterator<Item = f64>) -> f64 {
    let mut v: Vec<f64> = data.collect();
    v.sort_by(|a, b| a.total_cmp(b));
    if v.len() % 2 == 1 {
        v[v.len() / 2]
    } else {
        (v[v.len() / 2] + v[v.len() / 2 - 1]) / 2.
    }
}
