use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap};
use std::time::{Duration, Instant};

use indexmap::IndexSet;
use rand::prelude::*;
use rand_pcg::Pcg64;

use dslab_core::context::SimulationContext;
use dslab_core::log_warn;
use dslab_core::Id;

use dslab_dag::dag::DAG;
use dslab_dag::data_item::{DataTransferMode, DataTransferStrategy};
use dslab_dag::pareto::ParetoScheduler;
use dslab_dag::runner::Config;
use dslab_dag::scheduler::{Action, Scheduler, SchedulerParams, TimeSpan};
use dslab_dag::schedulers::common::{calc_ranks, evaluate_assignment, ScheduledTask};
use dslab_dag::schedulers::treap::Treap;
use dslab_dag::system::System;

pub struct CMSWCScheduler {
    data_transfer_strategy: DataTransferStrategy,
    n_schedules: usize,
    exploit_rate: f64,
    rng: RefCell<Pcg64>,
}

impl CMSWCScheduler {
    pub fn new(n_schedules: usize, exploit_rate: f64, seed: u64) -> Self {
        Self {
            data_transfer_strategy: DataTransferStrategy::Eager,
            n_schedules,
            exploit_rate,
            rng: RefCell::new(Pcg64::seed_from_u64(seed)),
        }
    }

    pub fn from_params(params: &SchedulerParams) -> Self {
        Self {
            data_transfer_strategy: params
                .get("data_transfer_strategy")
                .unwrap_or(DataTransferStrategy::Eager),
            n_schedules: params.get::<usize, &str>("n_schedules").unwrap(),
            exploit_rate: params.get::<f64, &str>("exploit_rate").unwrap(),
            rng: RefCell::new(Pcg64::seed_from_u64(params.get::<u64, &str>("seed").unwrap())),
        }
    }

    pub fn with_data_transfer_strategy(mut self, data_transfer_strategy: DataTransferStrategy) -> Self {
        self.data_transfer_strategy = data_transfer_strategy;
        self
    }

    fn schedule(&mut self, dag: &DAG, system: System, config: Config, ctx: &SimulationContext) -> Vec<Vec<Action>> {
        let start = Instant::now();
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

        for (task_seq_pos, task_id) in task_ids.iter().copied().enumerate() {
            //println!("{}", task_id);
            let mut new_schedules = Vec::new();
            for schedule in partial_schedules.into_iter() {
                let mut used = vec![false; resources.len()];
                for r in &schedule.task_resource {
                    if *r != resources.len() {
                        used[*r] = true;
                    }
                }
                let mut ban = vec![false; resources.len()];
                let mut hs = IndexSet::new();
                for (id, r) in resources.iter().enumerate() {
                    if !used[id] {
                        let tp = ((r.speed * 1000.).round() as i64, r.cores, r.memory);
                        if hs.contains(&tp) {
                            ban[id] = true;
                        } else {
                            hs.insert(tp);
                        }
                    }
                }
                for i in 0..resources.len() {
                    if ban[i] {
                        continue;
                    }
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

            let mut fronts =
                fast_non_dominated_sort(&new_schedules.iter().map(|s| (s.makespan, s.cost)).collect::<Vec<_>>());
            let new_size = new_schedules.len().min(self.n_schedules);
            let mut remain = Vec::with_capacity(new_size);
            for front in fronts.into_iter() {
                if remain.len() == new_size {
                    break;
                }
                if front.len() + remain.len() <= new_size {
                    for i in front.into_iter() {
                        remain.push(i);
                    }
                } else {
                    let tmp = front
                        .iter()
                        .map(|&i| (new_schedules[i].makespan, new_schedules[i].cost))
                        .collect::<Vec<_>>();
                    let keep = sde_density_selection(&tmp, new_size - remain.len());
                    for i in keep.into_iter() {
                        remain.push(front[i]);
                    }
                    break;
                }
            }
            remain.sort();
            remain.reverse();
            let mut new_new_schedules = Vec::with_capacity(new_size);
            for i in remain.into_iter() {
                new_new_schedules.push(new_schedules.swap_remove(i));
            }
            let extra = self.elite_study_strategy(
                dag,
                &system,
                &config,
                ctx,
                &new_new_schedules,
                &task_ids[..task_seq_pos + 1],
            );
            new_new_schedules.extend(extra);
            let tmp = new_new_schedules
                .iter()
                .map(|x| (x.makespan, x.cost))
                .collect::<Vec<_>>();
            let mut keep = sde_density_selection(&tmp, new_new_schedules.len().min(self.n_schedules));
            keep.sort();
            keep.reverse();
            partial_schedules = Vec::with_capacity(keep.len());
            for i in keep.into_iter() {
                partial_schedules.push(new_new_schedules.swap_remove(i));
            }
        }

        for s in &mut partial_schedules {
            s.actions.sort_by(|a, b| a.0.total_cmp(&b.0));
        }

        /*for s in &partial_schedules[..5] {
            println!("{:.3} {:.3}", s.makespan, s.cost);
        }*/

        for s in &mut partial_schedules {
            s.actions.sort_by(|a, b| a.0.total_cmp(&b.0));
        }

        println!("CMSWC elapsed secs = {:?}", start.elapsed());

        partial_schedules
            .into_iter()
            .map(|s| s.actions.into_iter().map(|x| x.1).collect::<Vec<_>>())
            .collect::<Vec<_>>()
    }

    fn elite_study_strategy<'a>(
        &'a self,
        dag: &'a DAG,
        system: &'a System,
        config: &'a Config,
        ctx: &'a SimulationContext,
        schedules: &[PartialSchedule<'_>],
        task_seq: &[usize],
    ) -> Vec<PartialSchedule<'a>> {
        let mut result = Vec::new();
        for s in schedules {
            let mut assignment = s.task_resource.clone();
            let t = self.rng.borrow_mut().gen_range(0f64..1f64);
            if t < self.exploit_rate {
                let step = self.rng.borrow_mut().gen_range(0..4);
                //println!("1 {}", step);
                match step {
                    0 => {
                        let rand_seq_pos = self.rng.borrow_mut().gen_range(0..task_seq.len());
                        let task = task_seq[rand_seq_pos];
                        let old = assignment[task];
                        let mut exist = IndexSet::new();
                        let task_ref = dag.get_task(task);
                        for r in assignment.iter().copied() {
                            if r < system.resources.len() && r != old && task_ref.is_allowed_on(r) {
                                exist.insert(r);
                            }
                        }
                        if !exist.is_empty() {
                            let exist_vec = exist.into_iter().collect::<Vec<_>>();
                            let r = exist_vec[self.rng.borrow_mut().gen_range(0..exist_vec.len())];
                            assignment[task] = r;
                            let mut new =
                                PartialSchedule::new(dag, self.data_transfer_strategy.clone(), system, config, ctx);
                            for id in task_seq {
                                new.assign_task(*id, assignment[*id]);
                            }
                            result.push(new);
                        }
                    }
                    1 => {
                        let rand_seq_pos = self.rng.borrow_mut().gen_range(0..task_seq.len());
                        let task = task_seq[rand_seq_pos];
                        let task_ref = dag.get_task(task);
                        let old = assignment[task];
                        let mut exist = IndexSet::new();
                        for r in assignment.iter().copied() {
                            exist.insert(r);
                        }
                        let mut allowed = Vec::new();
                        for r in 0..system.resources.len() {
                            if !exist.contains(&r) && task_ref.is_allowed_on(r) {
                                allowed.push(r);
                            }
                        }
                        if !allowed.is_empty() {
                            assignment[task] = allowed[self.rng.borrow_mut().gen_range(0..allowed.len())];
                            let mut new =
                                PartialSchedule::new(dag, self.data_transfer_strategy.clone(), system, config, ctx);
                            for id in task_seq {
                                new.assign_task(*id, assignment[*id]);
                            }
                            result.push(new);
                        }
                    }
                    2 => {
                        let rand_seq_pos1 = self.rng.borrow_mut().gen_range(0..task_seq.len());
                        let task1 = task_seq[rand_seq_pos1];
                        let mut possible = Vec::new();
                        for i in 0..task_seq.len() {
                            if assignment[i] != assignment[task1] && assignment[i] != system.resources.len() {
                                possible.push(i);
                            }
                        }
                        if !possible.is_empty() {
                            let task2 = possible[self.rng.borrow_mut().gen_range(0..possible.len())];
                            if dag.get_task(task1).is_allowed_on(assignment[task2])
                                && dag.get_task(task2).is_allowed_on(assignment[task1])
                            {
                                assignment.swap(task1, task2);
                                let mut new =
                                    PartialSchedule::new(dag, self.data_transfer_strategy.clone(), system, config, ctx);
                                for id in task_seq {
                                    new.assign_task(*id, assignment[*id]);
                                }
                                result.push(new);
                            }
                        }
                    }
                    3 => {
                        let rand_seq_pos1 = self.rng.borrow_mut().gen_range(0..task_seq.len());
                        let task1 = task_seq[rand_seq_pos1];
                        let mut stack = Vec::new();
                        let mut parents = IndexSet::new();
                        stack.push(task1);
                        let mut need_skip = IndexSet::new();
                        while let Some(v) = stack.pop() {
                            for input in dag.get_task(v).inputs.iter().copied() {
                                let item = dag.get_data_item(input);
                                if let Some(producer) = item.producer {
                                    if !parents.contains(&producer) {
                                        parents.insert(producer);
                                        stack.push(producer);
                                        need_skip.insert(producer);
                                    }
                                }
                            }
                        }
                        let mut possible = Vec::new();
                        for i in 0..rand_seq_pos1 {
                            if need_skip.contains(&task_seq[i]) {
                                need_skip.remove(&task_seq[i]);
                            }
                            if need_skip.is_empty()
                                && assignment[task_seq[i]] == assignment[task1]
                                && !parents.contains(&task_seq[i])
                            {
                                possible.push(i);
                            }
                        }
                        if !possible.is_empty() {
                            let pos2 = possible[self.rng.borrow_mut().gen_range(0..possible.len())];
                            let mut new_seq = task_seq.to_vec();
                            new_seq.remove(rand_seq_pos1);
                            new_seq.insert(pos2, task1);
                            let mut new =
                                PartialSchedule::new(dag, self.data_transfer_strategy.clone(), system, config, ctx);
                            for id in new_seq.into_iter() {
                                new.assign_task(id, assignment[id]);
                            }
                            result.push(new);
                        }
                    }
                    _ => {
                        unreachable!();
                    }
                }
            } else {
                let step = self.rng.borrow_mut().gen_range(0..2);
                //println!("2 {}", step);
                match step {
                    0 => {
                        let mut used = IndexSet::new();
                        for r in &assignment {
                            if *r != system.resources.len() {
                                used.insert(*r);
                            }
                        }
                        let used_vec = used.into_iter().collect::<Vec<_>>();
                        if used_vec.len() > 1 {
                            let r1 = used_vec[self.rng.borrow_mut().gen_range(0..used_vec.len())];
                            let mut r2 = r1;
                            while r2 == r1 {
                                r2 = used_vec[self.rng.borrow_mut().gen_range(0..used_vec.len())];
                            }
                            for (i, r) in assignment.iter_mut().enumerate() {
                                if *r == r1 {
                                    if dag.get_task(i).is_allowed_on(r2) {
                                        *r = r2;
                                    }
                                }
                            }
                            let mut new =
                                PartialSchedule::new(dag, self.data_transfer_strategy.clone(), system, config, ctx);
                            for id in task_seq {
                                new.assign_task(*id, assignment[*id]);
                            }
                            result.push(new);
                        }
                    }
                    1 => {
                        let mut used = IndexSet::new();
                        for r in &assignment {
                            if *r != system.resources.len() {
                                used.insert(*r);
                            }
                        }
                        let used_vec = used.iter().copied().collect::<Vec<_>>();
                        let r1 = used_vec[self.rng.borrow_mut().gen_range(0..used_vec.len())];
                        let mut others = Vec::new();
                        for r in 0..system.resources.len() {
                            if !used.contains(&r) {
                                let res1 = &system.resources[r1];
                                let res2 = &system.resources[r];
                                if res1.memory == res2.memory
                                    && res1.speed == res2.speed
                                    && res1.cores == res2.cores
                                    && res1.price == res2.price
                                {
                                    others.push(r);
                                }
                            }
                        }
                        if !others.is_empty() {
                            let r2 = others[self.rng.borrow_mut().gen_range(0..others.len())];
                            for (i, r) in assignment.iter_mut().enumerate() {
                                if *r == r1 {
                                    if dag.get_task(i).is_allowed_on(r2) && self.rng.borrow_mut().gen_range(0..2) == 1 {
                                        *r = r2;
                                    }
                                }
                            }
                            let mut new =
                                PartialSchedule::new(dag, self.data_transfer_strategy.clone(), system, config, ctx);
                            for id in task_seq {
                                new.assign_task(*id, assignment[*id]);
                            }
                            result.push(new);
                        }
                    }
                    _ => {
                        unreachable!();
                    }
                }
            }
        }
        result
    }
}

impl ParetoScheduler for CMSWCScheduler {
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
            "CMSWCScheduler doesn't support DataTransferMode::Manual"
        );

        if dag.get_tasks().iter().any(|task| task.min_cores != task.max_cores) {
            log_warn!(
                ctx,
                "some tasks support different number of cores, but CMSWC will always use min_cores"
            );
        }

        self.schedule(dag, system, config, ctx)
    }
}

impl Scheduler for CMSWCScheduler {
    fn start(&mut self, dag: &DAG, system: System, config: Config, ctx: &SimulationContext) -> Vec<Action> {
        self.find_pareto_front(dag, system, config, ctx).swap_remove(0)
    }

    fn is_static(&self) -> bool {
        true
    }
}

#[derive(Clone)]
pub struct PartialSchedule<'a> {
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
    pub task_resource: Vec<usize>,
    pub scheduled_tasks: Vec<Vec<BTreeSet<ScheduledTask>>>,
    pub resource_start: Vec<f64>,
    pub resource_end: Vec<f64>,
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
            task_resource: vec![system.resources.len(); dag.get_tasks().len()],
            scheduled_tasks: system
                .resources
                .iter()
                .map(|resource| (0..resource.cores_available).map(|_| BTreeSet::new()).collect())
                .collect(),
            resource_start: vec![f64::INFINITY; system.resources.len()],
            resource_end: vec![-f64::INFINITY; system.resources.len()],
            makespan: 0.0,
            cost: 0.0,
        }
    }

    pub fn assign_task(&mut self, task: usize, resource: usize) {
        assert!(self.dag.get_task(task).is_allowed_on(resource));
        self.task_resource[task] = resource;
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
        self.cost -= self.compute_resource_cost(resource);
        self.resource_start[resource] = self.resource_start[resource].min(start_time);
        self.resource_end[resource] = self.resource_end[resource].max(finish_time);
        for item_id in self.dag.get_task(task).inputs.iter().copied() {
            let item = self.dag.get_data_item(item_id);
            if let Some(producer) = item.producer {
                assert!(self.task_locations.contains_key(&producer)); // parents must be scheduled
                let prev_resource = self.task_resource[producer];
                // TODO: properly update master node in DataTransferMode::ViaMasterNode (note that the paper uses DataTransferMode::Direct)
                match self.data_transfer {
                    DataTransferStrategy::Eager => {
                        let transfer = item.size
                            * self.config.data_transfer_mode.net_time(
                                self.system.network,
                                self.system.resources[prev_resource].id,
                                self.system.resources[resource].id,
                                self.ctx.id(),
                            );
                        self.resource_start[resource] = self.resource_start[resource].min(self.finish_time[producer]);
                        if prev_resource != resource {
                            self.cost -= self.compute_resource_cost(prev_resource);
                            self.resource_end[prev_resource] =
                                self.resource_end[prev_resource].max(self.finish_time[producer] + transfer);
                            self.cost += self.compute_resource_cost(prev_resource);
                        }
                    }
                    DataTransferStrategy::Lazy => {
                        let download_time = match self.config.data_transfer_mode {
                            DataTransferMode::ViaMasterNode => {
                                self.system
                                    .network
                                    .latency(self.ctx.id(), self.system.resources[resource].id)
                                    + item.size
                                        / self
                                            .system
                                            .network
                                            .bandwidth(self.ctx.id(), self.system.resources[resource].id)
                            }
                            DataTransferMode::Direct => {
                                if prev_resource == resource {
                                    0.
                                } else {
                                    self.system.network.latency(
                                        self.system.resources[prev_resource].id,
                                        self.system.resources[resource].id,
                                    ) + item.size
                                        / self.system.network.bandwidth(
                                            self.system.resources[prev_resource].id,
                                            self.system.resources[resource].id,
                                        )
                                }
                            }
                            DataTransferMode::Manual => 0.,
                        };
                        if prev_resource != resource {
                            self.cost -= self.compute_resource_cost(prev_resource);
                            self.resource_end[prev_resource] =
                                self.resource_end[prev_resource].max(download_time + start_time);
                            self.cost += self.compute_resource_cost(prev_resource);
                        }
                    }
                }
            }
        }
        self.cost += self.compute_resource_cost(resource);
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

    fn compute_resource_cost(&self, resource: usize) -> f64 {
        if self.resource_end[resource].is_infinite() || self.resource_start[resource].is_infinite() {
            return 0.;
        }
        let duration = self.resource_end[resource] - self.resource_start[resource];
        let n_intervals = (duration - 1e-9).div_euclid(self.config.pricing_interval) + 1.0;
        n_intervals * self.system.resources[resource].price
    }
}

fn dominates(obj1: (f64, f64), obj2: (f64, f64)) -> bool {
    (obj1.0 <= obj2.0 && obj1.1 < obj2.1) || (obj1.0 < obj2.0 && obj1.1 <= obj2.1)
}

fn fast_non_dominated_sort(schedules: &[(f64, f64)]) -> Vec<Vec<usize>> {
    let mut fronts = Vec::new();
    let mut s_dom = vec![Vec::new(); schedules.len()];
    let mut ctr = vec![0; schedules.len()];
    let mut front = Vec::new();
    for (i, p) in schedules.iter().enumerate() {
        for (j, q) in schedules.iter().enumerate() {
            if dominates(*p, *q) {
                s_dom[i].push(j);
            } else if dominates(*q, *p) {
                ctr[i] += 1;
            }
        }
        if ctr[i] == 0 {
            front.push(i);
        }
    }
    while !front.is_empty() {
        fronts.push(front.clone());
        let mut new_front = Vec::new();
        for i in front.into_iter() {
            for j in s_dom[i].iter().copied() {
                ctr[j] -= 1;
                if ctr[j] == 0 {
                    new_front.push(j);
                }
            }
        }
        front = new_front;
    }
    fronts
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

pub fn sde_density_selection(objectives: &[(f64, f64)], keep: usize) -> Vec<usize> {
    let mut dist = vec![0f64; objectives.len()];
    let mut order = (0..objectives.len()).collect::<Vec<_>>();
    order.sort_by(|a, b| objectives[*a].0.total_cmp(&objectives[*b].0));
    dist[order[0]] = f64::INFINITY;
    dist[order[order.len() - 1]] = f64::INFINITY;
    let mut range = objectives[*order.last().unwrap()].0 - objectives[order[0]].0;
    for i in 1..order.len() - 1 {
        dist[order[i]] = (objectives[order[i + 1]].0 - objectives[order[i]].0) / range;
    }
    order.sort_by(|a, b| objectives[*a].1.total_cmp(&objectives[*b].1));
    let mut range = objectives[*order.last().unwrap()].1 - objectives[order[0]].1;
    dist[order[0]] = f64::INFINITY;
    dist[order[order.len() - 1]] = f64::INFINITY;
    for i in 1..order.len() - 1 {
        dist[order[i]] += (objectives[order[i + 1]].1 - objectives[order[i]].1) / range;
    }
    order.sort_by(|a, b| dist[*a].total_cmp(&dist[*b]).reverse());
    order[..keep].to_vec()
}
