use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap};
use std::time::{Duration, Instant};

use indexmap::IndexSet;
use rand::distributions::WeightedIndex;
use rand::prelude::*;
use rand_pcg::Pcg64;

use dslab_core::context::SimulationContext;
use dslab_core::log_warn;
use dslab_core::Id;

use dslab_dag::dag::DAG;
use dslab_dag::data_item::{DataTransferMode, DataTransferStrategy};
use dslab_dag::pareto::ParetoScheduler;
use dslab_dag::pareto_schedulers::moheft::PartialSchedule;
use dslab_dag::runner::Config;
use dslab_dag::scheduler::{Action, Scheduler, SchedulerParams, TimeSpan};
use dslab_dag::schedulers::common::{calc_ranks, evaluate_assignment, ScheduledTask};
use dslab_dag::schedulers::treap::Treap;
use dslab_dag::system::System;

pub struct VMALSScheduler {
    beta: i32,
    data_transfer_strategy: DataTransferStrategy,
    evaporation: f64,
    n_schedules: usize,
    q0: f64,
    rng: Pcg64,
    time_limit: Duration,
    vns_no_improvement_max: usize,
    vns_sample_rate: f64,
}

impl VMALSScheduler {
    pub fn new(
        n_schedules: usize,
        beta: i32,
        evaporation: f64,
        q0: f64,
        seed: u64,
        time_limit: Duration,
        vns_no_improvement_max: usize,
        vns_sample_rate: f64,
    ) -> Self {
        Self {
            beta,
            data_transfer_strategy: DataTransferStrategy::Eager,
            evaporation,
            n_schedules,
            q0,
            rng: Pcg64::seed_from_u64(seed),
            time_limit,
            vns_no_improvement_max,
            vns_sample_rate,
        }
    }

    pub fn from_params(params: &SchedulerParams) -> Self {
        Self {
            beta: params.get::<i32, &str>("beta").unwrap(),
            data_transfer_strategy: params
                .get("data_transfer_strategy")
                .unwrap_or(DataTransferStrategy::Eager),
            evaporation: params.get::<f64, &str>("evaporation").unwrap(),
            n_schedules: params.get::<usize, &str>("n_schedules").unwrap(),
            q0: params.get::<f64, &str>("q0").unwrap(),
            rng: Pcg64::seed_from_u64(params.get::<u64, &str>("seed").unwrap()),
            time_limit: Duration::from_secs_f64(params.get::<f64, &str>("time_limit").unwrap()),
            vns_no_improvement_max: params.get::<usize, &str>("vns_no_improvement_max").unwrap(),
            vns_sample_rate: params.get::<f64, &str>("vns_sample_rate").unwrap(),
        }
    }

    pub fn with_data_transfer_strategy(mut self, data_transfer_strategy: DataTransferStrategy) -> Self {
        self.data_transfer_strategy = data_transfer_strategy;
        self
    }

    fn schedule(&mut self, dag: &DAG, system: System, config: Config, ctx: &SimulationContext) -> Vec<Vec<Action>> {
        let (mincost, heftspan, default_pheromone) = self.compute_default_pheromone(dag, &system, &config, ctx);

        let start = Instant::now();
        let avg_net_time = system.avg_net_time(ctx.id(), &config.data_transfer_mode);

        let mut heur = calc_ranks(system.avg_flop_time(), avg_net_time, dag);
        for x in &mut heur {
            if *x == 0. {
                *x = 1e-6;
            } else {
                *x = x.powi(self.beta);
            }
        }

        let n_tasks = dag.get_tasks().len();
        let mut pheromone = vec![vec![default_pheromone; n_tasks]; n_tasks + 1];

        let mut archive: Vec<Solution> = Vec::new();

        let wdom = |a: &PartialSchedule<'_>, b: &PartialSchedule<'_>| a.makespan <= b.makespan && a.cost <= b.cost;
        let sdom = |a: &PartialSchedule<'_>, b: &PartialSchedule<'_>| {
            a.makespan <= b.makespan && a.cost <= b.cost && !(a.makespan == b.makespan && a.cost == b.cost)
        };

        let mut loops = 0;
        while start.elapsed() < self.time_limit {
            loops += 1;
            let mut seq = vec![Vec::with_capacity(n_tasks); self.n_schedules];
            let mut deg = vec![vec![0; n_tasks]; self.n_schedules];
            let mut ready = vec![IndexSet::new(); self.n_schedules];
            for j in 0..self.n_schedules {
                for i in 0..n_tasks {
                    let task = dag.get_task(i);
                    for input in &task.inputs {
                        let item = dag.get_data_item(*input);
                        if item.producer.is_some() {
                            deg[j][i] += 1;
                        }
                    }
                    if deg[j][i] == 0 {
                        ready[j].insert(i);
                    }
                }
            }
            let mut prv = vec![n_tasks; self.n_schedules];
            for _ in 0..n_tasks {
                for j in 0..self.n_schedules {
                    let p = ready[j]
                        .iter()
                        .map(|&i| pheromone[prv[j]][i] * heur[i])
                        .collect::<Vec<_>>();
                    let q = self.rng.gen_range(0f64..1f64);
                    let mut nxt = usize::MAX;
                    if q <= self.q0 {
                        let mut best_val = -f64::INFINITY;
                        let mut best = usize::MAX;
                        for (x, val) in ready[j].iter().zip(p.iter()) {
                            if best_val < *val {
                                best_val = *val;
                                best = *x;
                            }
                        }
                        nxt = best;
                    } else {
                        let dist = WeightedIndex::new(&p).unwrap();
                        let pos = dist.sample(&mut self.rng);
                        nxt = *ready[j].iter().skip(pos).next().unwrap();
                    }
                    pheromone[prv[j]][nxt] =
                        (1. - self.evaporation) * pheromone[prv[j]][nxt] + self.evaporation * default_pheromone;
                    prv[j] = nxt;
                    ready[j].remove(&prv[j]);
                    seq[j].push(prv[j]);
                    let task = dag.get_task(prv[j]);
                    for output in &task.outputs {
                        let item = dag.get_data_item(*output);
                        for consumer in &item.consumers {
                            deg[j][*consumer] -= 1;
                            if deg[j][*consumer] == 0 {
                                ready[j].insert(*consumer);
                            }
                        }
                    }
                }
            }
            for j in 0..self.n_schedules {
                let w = self.rng.gen_range(0f64..1f64);
                let sched = self.build_schedule(dag, &system, &config, ctx, w, heftspan, mincost, &seq[j]);
                let mut ptr = 0;
                let mut dom = false;
                while ptr < archive.len() {
                    if wdom(&archive[ptr].sched, &sched) {
                        dom = true;
                        break;
                    }
                    if wdom(&sched, &archive[ptr].sched) {
                        archive.swap_remove(ptr);
                        continue;
                    }
                    ptr += 1;
                }
                if !dom {
                    archive.push(Solution::new(sched, seq[j].clone()));
                }
            }
            let mut n_sols = ((archive.len() as f64) * self.vns_sample_rate).ceil() as usize;
            let mut cnt = 1;
            loop {
                let mut sol = archive[self.rng.gen_range(0..archive.len())].clone();
                let mut l = 1;
                while l <= self.vns_no_improvement_max {
                    let mut neigh = 0;
                    while neigh <= 1 && l <= self.vns_no_improvement_max {
                        let mut new_sol = sol.clone();
                        if neigh == 0 {
                            let other = &archive[self.rng.gen_range(0..archive.len())];
                            let pos = self.rng.gen_range(0..n_tasks);
                            let mut new_seq = Vec::with_capacity(n_tasks);
                            let mut exist = IndexSet::new();
                            for x in &new_sol.seq[..pos + 1] {
                                new_seq.push(*x);
                                exist.insert(*x);
                            }
                            for x in &other.seq {
                                if !exist.contains(x) {
                                    new_seq.push(*x);
                                }
                            }
                            let w = self.rng.gen_range(0f64..1f64);
                            let new_sched =
                                self.build_schedule(dag, &system, &config, ctx, w, heftspan, mincost, &new_seq);
                            new_sol = Solution::new(new_sched, new_seq);
                        } else {
                            let mut pos_vec = vec![0; n_tasks];
                            for (i, x) in new_sol.seq.iter().enumerate() {
                                pos_vec[*x] = i;
                            }
                            let pos = self.rng.gen_range(0..n_tasks);
                            let first_task = new_sol.seq[pos];
                            let mut first_pred = IndexSet::new();
                            let mut first_succ = IndexSet::new();
                            for i in &dag.get_task(first_task).inputs {
                                let item = dag.get_data_item(*i);
                                if let Some(p) = item.producer {
                                    first_pred.insert(p);
                                }
                            }
                            for i in &dag.get_task(first_task).outputs {
                                let item = dag.get_data_item(*i);
                                for c in &item.consumers {
                                    first_succ.insert(*c);
                                }
                            }
                            let l = first_pred.iter().map(|&i| pos_vec[i] + 1).max().unwrap_or(0);
                            let r = first_succ.iter().map(|&i| pos_vec[i]).min().unwrap_or(n_tasks);
                            if r - l > 1 {
                                let mut new_seq = new_sol.seq.clone();
                                let mut new_pos = pos;
                                while new_pos == pos {
                                    new_pos = self.rng.gen_range(l..r);
                                }
                                let mut deg = vec![0; n_tasks];
                                let mut ready = IndexSet::new();
                                for d in l..r {
                                    if new_seq[d] != first_task {
                                        let task = dag.get_task(new_seq[d]);
                                        for input in &task.inputs {
                                            let item = dag.get_data_item(*input);
                                            if let Some(producer) = item.producer {
                                                if pos_vec[producer] >= l {
                                                    deg[new_seq[d]] += 1;
                                                }
                                            }
                                        }
                                        if deg[new_seq[d]] == 0 {
                                            ready.insert(new_seq[d]);
                                        }
                                    }
                                }
                                new_seq[new_pos] = first_task;
                                for d in l..r {
                                    if d != new_pos {
                                        assert!(!ready.is_empty());
                                        let pos = self.rng.gen_range(0..ready.len());
                                        let id = *ready.iter().skip(pos).next().unwrap();
                                        ready.remove(&id);
                                        new_seq[d] = id;
                                        let task = dag.get_task(id);
                                        for output in &task.outputs {
                                            let item = dag.get_data_item(*output);
                                            for c in &item.consumers {
                                                if pos_vec[*c] >= l && pos_vec[*c] < r {
                                                    deg[*c] -= 1;
                                                    if deg[*c] == 0 {
                                                        ready.insert(*c);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                let w = self.rng.gen_range(0f64..1f64);
                                let new_sched =
                                    self.build_schedule(dag, &system, &config, ctx, w, heftspan, mincost, &new_seq);
                                new_sol = Solution::new(new_sched, new_seq);
                            }
                        }
                        let mut dom = false;
                        for s in &archive {
                            if wdom(&s.sched, &new_sol.sched) {
                                dom = true;
                                break;
                            }
                        }
                        if dom {
                            l += 1;
                            neigh += 1;
                        } else {
                            let mut ptr = 0;
                            while ptr < archive.len() {
                                if wdom(&new_sol.sched, &archive[ptr].sched) {
                                    archive.swap_remove(ptr);
                                    continue;
                                }
                                ptr += 1;
                            }
                            archive.push(new_sol.clone());
                            sol = new_sol;
                            neigh = 0;
                            l = 1;
                        }
                    }
                }
                cnt += 1;
                n_sols = ((archive.len() as f64) * self.vns_sample_rate).ceil() as usize;
                if cnt >= n_sols {
                    break;
                }
            }
            let best = &archive[self.rng.gen_range(0..archive.len())];
            let mut prv = n_tasks;
            let val = 1f64 / (best.sched.makespan * best.sched.cost);
            for v in &mut pheromone {
                for x in v.iter_mut() {
                    *x *= 1. - self.evaporation;
                }
            }
            for x in best.seq.iter().copied() {
                pheromone[prv][x] += val * self.evaporation;
                prv = x;
            }
        }
        /*println!("VMALS raw data:");
        for x in &archive {
            println!("{:.3} {:.3}", x.sched.makespan, x.sched.cost);
        }*/
        for s in &mut archive {
            s.sched.actions.sort_by(|a, b| a.0.total_cmp(&b.0));
        }
        archive
            .iter()
            .map(|x| x.sched.actions.clone().into_iter().map(|x| x.1).collect::<Vec<_>>())
            .collect::<Vec<_>>()
    }

    fn compute_default_pheromone(
        &self,
        dag: &DAG,
        system: &System,
        config: &Config,
        ctx: &SimulationContext,
    ) -> (f64, f64, f64) {
        let mut mincost = 0f64;
        let mut min_price = f64::INFINITY;
        let mut speed = 1f64;
        for r in system.resources {
            if r.price < min_price && r.price != 0. {
                min_price = r.price;
                speed = r.speed;
            }
        }
        for task in dag.get_tasks() {
            mincost += task.flops / speed;
        }
        mincost = min_price * ((mincost - 1e-9).div_euclid(config.pricing_interval) + 1.0);
        let resources = system.resources;
        let network = system.network;

        let avg_net_time = system.avg_net_time(ctx.id(), &config.data_transfer_mode);

        let task_count = dag.get_tasks().len();

        let task_ranks = calc_ranks(system.avg_flop_time(), avg_net_time, dag);
        let mut task_ids = (0..task_count).collect::<Vec<_>>();
        task_ids.sort_by(|&a, &b| task_ranks[b].total_cmp(&task_ranks[a]));

        let mut task_finish_times = vec![0.; task_count];
        let mut scheduled_tasks: Vec<Vec<BTreeSet<ScheduledTask>>> = resources
            .iter()
            .map(|resource| (0..resource.cores_available).map(|_| BTreeSet::new()).collect())
            .collect();
        let mut memory_usage: Vec<Treap> = (0..resources.len()).map(|_| Treap::new()).collect();
        let mut data_locations: HashMap<usize, Id> = HashMap::new();
        let mut task_locations: HashMap<usize, Id> = HashMap::new();

        for task_id in task_ids.into_iter() {
            let mut best_finish = -1.;
            let mut best_start = -1.;
            let mut best_resource = 0;
            let mut best_cores: Vec<u32> = Vec::new();
            let mut max_speed = -f64::INFINITY;
            for resource in 0..resources.len() {
                if dag.get_task(task_id).is_allowed_on(resource) && resources[resource].speed > max_speed {
                    max_speed = resources[resource].speed;
                }
            }
            for resource in 0..resources.len() {
                if resources[resource].speed != max_speed {
                    continue;
                }
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

                if best_finish == -1. || best_finish > finish_time {
                    best_start = start_time;
                    best_finish = finish_time;
                    best_resource = resource;
                    best_cores = cores;
                }
            }

            assert_ne!(best_finish, -1.);

            task_finish_times[task_id] = best_finish;
            for &core in best_cores.iter() {
                scheduled_tasks[best_resource][core as usize].insert(ScheduledTask::new(
                    best_start,
                    best_finish,
                    task_id,
                ));
            }
            memory_usage[best_resource].add(best_start, best_finish, dag.get_task(task_id).memory);
            for &output in dag.get_task(task_id).outputs.iter() {
                data_locations.insert(output, resources[best_resource].id);
            }
            task_locations.insert(task_id, resources[best_resource].id);
        }
        let makespan = task_finish_times
            .iter()
            .copied()
            .max_by(|&a, &b| a.total_cmp(&b))
            .unwrap();
        (mincost, makespan, 1f64 / (makespan * mincost * (task_count as f64)))
    }

    fn build_schedule<'a>(
        &self,
        dag: &'a DAG,
        system: &'a System,
        config: &'a Config,
        ctx: &'a SimulationContext,
        weight: f64,
        heftspan: f64,
        mincost: f64,
        seq: &[usize],
    ) -> PartialSchedule<'a> {
        let mut sched = PartialSchedule::new(dag, self.data_transfer_strategy.clone(), system, config, ctx);
        for t in seq.iter().copied() {
            let mut best_fitness = f64::INFINITY;
            let mut best_next = sched.clone();
            for r in 0..system.resources.len() {
                if dag.get_task(t).is_allowed_on(r) {
                    let mut tmp = sched.clone();
                    tmp.assign_task(t, r);
                    let fitness = tmp.makespan / heftspan * weight + tmp.cost / mincost * (1. - weight);
                    if fitness < best_fitness {
                        best_fitness = fitness;
                        best_next = tmp;
                    }
                }
            }
            assert!(best_fitness.is_finite());
            sched = best_next;
        }
        sched
    }
}

impl ParetoScheduler for VMALSScheduler {
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
            "VMALSScheduler doesn't support DataTransferMode::Manual"
        );

        if dag.get_tasks().iter().any(|task| task.min_cores != task.max_cores) {
            log_warn!(
                ctx,
                "some tasks support different number of cores, but VMALS will always use min_cores"
            );
        }

        self.schedule(dag, system, config, ctx)
    }
}

#[derive(Clone)]
struct Solution<'a> {
    pub sched: PartialSchedule<'a>,
    pub seq: Vec<usize>,
}

impl<'a> Solution<'a> {
    pub fn new(sched: PartialSchedule<'a>, seq: Vec<usize>) -> Self {
        Self { sched, seq }
    }
}
