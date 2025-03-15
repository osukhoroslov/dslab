use simcore::context::SimulationContext;
use simcore::log_warn;

use crate::dag::DAG;
use crate::data_item::{DataTransferMode, DataTransferStrategy};
use crate::pareto::ParetoScheduler;
use crate::runner::Config;
use crate::scheduler::{Action, SchedulerParams};
use crate::schedulers::common::calc_ranks;
use crate::schedulers::pareto::common::*;
use crate::system::System;

pub struct MOHeftScheduler {
    data_transfer_strategy: DataTransferStrategy,
    n_schedules: usize,
}

impl MOHeftScheduler {
    pub fn new(n_schedules: usize) -> Self {
        Self {
            data_transfer_strategy: DataTransferStrategy::Eager,
            n_schedules,
        }
    }

    pub fn from_params(params: &SchedulerParams) -> Self {
        Self {
            data_transfer_strategy: params
                .get("data_transfer_strategy")
                .unwrap_or(DataTransferStrategy::Eager),
            n_schedules: params.get::<usize, &str>("n_schedules").unwrap(),
        }
    }

    pub fn with_data_transfer_strategy(mut self, data_transfer_strategy: DataTransferStrategy) -> Self {
        self.data_transfer_strategy = data_transfer_strategy;
        self
    }

    fn schedule(&mut self, dag: &DAG, system: System, config: Config, ctx: &SimulationContext) -> Vec<Vec<Action>> {
        let resources = system.resources;

        let avg_net_time = system.avg_net_time(ctx.id(), &config.data_transfer_mode);

        let task_count = dag.get_tasks().len();

        let task_ranks = calc_ranks(system.avg_flop_time(), avg_net_time, dag);
        let mut task_ids = (0..task_count).collect::<Vec<_>>();
        task_ids.sort_by(|&a, &b| task_ranks[b].total_cmp(&task_ranks[a]));

        let mut partial_schedules = vec![PartialSchedule::new(
            dag,
            self.data_transfer_strategy.clone(),
            &system,
            &config,
            ctx,
        )];

        for task_id in task_ids.into_iter() {
            let mut schedules = Vec::new();
            for schedule in partial_schedules.into_iter() {
                for (i, r) in resources.iter().enumerate() {
                    let need_cores = dag.get_task(task_id).min_cores;
                    if r.compute.borrow().cores_total() < need_cores {
                        continue;
                    }
                    let need_memory = dag.get_task(task_id).memory;
                    if r.compute.borrow().memory_total() < need_memory {
                        continue;
                    }
                    if !dag.get_task(task_id).is_allowed_on(i) {
                        continue;
                    }
                    let mut new_schedule = schedule.clone();
                    new_schedule.assign_task(task_id, i);
                    schedules.push(new_schedule);
                }
            }

            let mut remain = select_nondominated(&schedules.iter().map(|s| (s.makespan, s.cost)).collect::<Vec<_>>());
            remain.sort();
            remain.reverse();
            let mut new_schedules = Vec::with_capacity(remain.len());
            for i in remain.into_iter() {
                new_schedules.push(schedules.swap_remove(i));
            }
            let mut dist =
                compute_crowding_distance(&new_schedules.iter().map(|s| (s.makespan, s.cost)).collect::<Vec<_>>())
                    .into_iter()
                    .enumerate()
                    .collect::<Vec<_>>();
            dist.sort_by(|a, b| a.1.total_cmp(&b.1).reverse());
            let size = self.n_schedules.min(dist.len());
            partial_schedules = Vec::with_capacity(size);
            let mut indices = dist.drain(..size).map(|x| x.0).collect::<Vec<_>>();
            indices.sort();
            indices.reverse();
            for i in indices.into_iter() {
                partial_schedules.push(new_schedules.swap_remove(i));
            }
        }

        for s in &mut partial_schedules {
            s.actions.sort_by(|a, b| a.0.total_cmp(&b.0));
        }

        partial_schedules
            .into_iter()
            .map(|s| s.actions.into_iter().map(|x| x.1).collect::<Vec<_>>())
            .collect::<Vec<_>>()
    }
}

impl ParetoScheduler for MOHeftScheduler {
    fn find_pareto_front(
        &mut self,
        dag: &DAG,
        system: System,
        config: Config,
        ctx: &SimulationContext,
    ) -> Vec<Vec<Action>> {
        assert_ne!(
            config.data_transfer_mode,
            DataTransferMode::Manual,
            "MOHeftScheduler doesn't support DataTransferMode::Manual"
        );

        if dag.get_tasks().iter().any(|task| task.min_cores != task.max_cores) {
            log_warn!(
                ctx,
                "some tasks support different number of cores, but MOHEFT will always use min_cores"
            );
        }

        self.schedule(dag, system, config, ctx)
    }
}
