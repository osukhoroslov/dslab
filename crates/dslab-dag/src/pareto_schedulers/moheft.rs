use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};

use dslab_core::context::SimulationContext;
use dslab_core::log_warn;
use dslab_core::Id;

use crate::dag::DAG;
use crate::data_item::{DataTransferMode, DataTransferStrategy};
use crate::pareto::ParetoScheduler;
use crate::runner::Config;
use crate::scheduler::{Action, Scheduler, SchedulerParams, TimeSpan};
use crate::schedulers::common::{calc_ranks, evaluate_assignment, ScheduledTask};
use crate::schedulers::treap::Treap;
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
            let mut new_schedules = Vec::new();
            for schedule in partial_schedules.into_iter() {
                for i in 0..resources.len() {
                    let need_cores = dag.get_task(task_id).min_cores;
                    if resources[i].compute.borrow().cores_total() < need_cores {
                        continue;
                    }
                    let need_memory = dag.get_task(task_id).memory;
                    if resources[i].compute.borrow().memory_total() < need_memory {
                        continue;
                    }
                    if !dag.get_task(task_id).is_allowed_on(i) {
                        continue;
                    }
                    let mut new_schedule = schedule.clone();
                    new_schedule.assign_task(task_id, i);
                    new_schedules.push(new_schedule);
                }
            }

            let mut remain =
                select_nondominated(&new_schedules.iter().map(|s| (s.makespan, s.cost)).collect::<Vec<_>>());
            remain.sort();
            remain.reverse();
            let mut new_new_schedules = Vec::with_capacity(remain.len());
            for i in remain.into_iter() {
                new_new_schedules.push(new_schedules.swap_remove(i));
            }
            /*println!("I have schedules:");
            for s in &new_new_schedules {
                println!("{} {}", s.makespan, s.cost);
            }*/
            let mut dist = compute_crowding_distance(
                &new_new_schedules
                    .iter()
                    .map(|s| (s.makespan, s.cost))
                    .collect::<Vec<_>>(),
            )
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
                partial_schedules.push(new_new_schedules.swap_remove(i));
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

impl Scheduler for MOHeftScheduler {
    fn start(&mut self, dag: &DAG, system: System, config: Config, ctx: &SimulationContext) -> Vec<Action> {
        self.find_pareto_front(dag, system, config, ctx).swap_remove(0)
    }

    fn is_static(&self) -> bool {
        true
    }
}

#[derive(Clone)]
struct PartialSchedule<'a> {
    dag: &'a DAG,
    data_transfer: DataTransferStrategy,
    system: &'a System<'a>,
    config: &'a Config,
    ctx: &'a SimulationContext,
    pub actions: Vec<(f64, Action)>,
    pub finish_time: Vec<f64>,
    pub memory_usage: Vec<Treap>,
    pub data_locations: HashMap<usize, Id>,
    pub task_locations: HashMap<usize, Id>,
    pub scheduled_tasks: Vec<Vec<BTreeSet<ScheduledTask>>>,
    pub resource_start: Vec<f64>,
    pub resource_end: Vec<f64>,
    pub resource_cost: Vec<f64>,
    pub makespan: f64,
    pub cost: f64,
}

impl<'a> PartialSchedule<'a> {
    pub fn new(
        dag: &'a DAG,
        data_transfer: DataTransferStrategy,
        system: &'a System<'a>,
        config: &'a Config,
        ctx: &'a SimulationContext,
    ) -> Self {
        Self {
            dag,
            data_transfer,
            system,
            config,
            ctx,
            actions: Vec::new(),
            finish_time: vec![0f64; dag.get_tasks().len()],
            memory_usage: (0..system.resources.len()).map(|_| Treap::new()).collect(),
            data_locations: HashMap::new(),
            task_locations: HashMap::new(),
            scheduled_tasks: system
                .resources
                .iter()
                .map(|resource| (0..resource.cores_available).map(|_| BTreeSet::new()).collect())
                .collect(),
            resource_start: vec![f64::INFINITY; system.resources.len()],
            resource_end: vec![-f64::INFINITY; system.resources.len()],
            resource_cost: vec![0f64; system.resources.len()],
            makespan: 0.0,
            cost: 0.0,
        }
    }

    pub fn assign_task(&mut self, task: usize, resource: usize) {
        let res = evaluate_assignment(
            task,
            resource,
            &self.finish_time,
            &self.scheduled_tasks,
            &self.memory_usage,
            &self.data_locations,
            &self.task_locations,
            &self.data_transfer,
            self.dag,
            self.system.resources,
            self.system.network,
            self.config,
            self.ctx,
        );
        assert!(res.is_some());
        let (start_time, finish_time, cores) = res.unwrap();
        self.makespan = self.makespan.max(finish_time);
        self.finish_time[task] = finish_time;
        for &core in cores.iter() {
            self.scheduled_tasks[resource][core as usize].insert(ScheduledTask::new(start_time, finish_time, task));
        }
        self.memory_usage[resource].add(start_time, finish_time, self.dag.get_task(task).memory);
        for &output in self.dag.get_task(task).outputs.iter() {
            self.data_locations.insert(output, self.system.resources[resource].id);
        }
        self.cost -= self.resource_cost[resource];
        self.resource_start[resource] = self.resource_start[resource].min(start_time);
        self.resource_end[resource] = self.resource_end[resource].max(finish_time);
        self.resource_cost[resource] = self.resource_end[resource] - self.resource_start[resource];
        self.cost += self.resource_cost[resource];
        self.task_locations.insert(task, self.system.resources[resource].id);
        self.actions.push((
            start_time,
            Action::ScheduleTaskOnCores {
                task,
                resource,
                cores,
                expected_span: Some(TimeSpan::new(start_time, finish_time)),
            },
        ));
    }
}

/// Returns ordered sequence of nondominated indices.
pub fn select_nondominated(objectives: &[(f64, f64)]) -> Vec<usize> {
    let mut ind = (0..objectives.len()).collect::<Vec<_>>();
    ind.sort_by(|a, b| {
        objectives[*a]
            .0
            .total_cmp(&objectives[*b].0)
            .then(objectives[*a].1.total_cmp(&objectives[*b].1))
    });
    let mut result = Vec::new();
    let mut min_second = f64::INFINITY;
    for i in ind.into_iter() {
        let obj = objectives[i];
        if obj.1 < min_second {
            min_second = obj.1;
            result.push(i);
        }
    }
    result
}

/// Assumes that no points are dominated by others.
pub fn compute_crowding_distance(objectives: &[(f64, f64)]) -> Vec<f64> {
    let mut dist = vec![0f64; objectives.len()];
    let mut order = (0..objectives.len()).collect::<Vec<_>>();
    order.sort_by(|a, b| objectives[*a].0.total_cmp(&objectives[*b].0));
    dist[order[0]] = f64::INFINITY;
    dist[order[order.len() - 1]] = f64::INFINITY;
    for i in 1..order.len() - 1 {
        dist[order[i]] = objectives[order[i + 1]].0 - objectives[order[i - 1]].0 + objectives[order[i - 1]].1
            - objectives[order[i + 1]].1;
    }
    dist
}
