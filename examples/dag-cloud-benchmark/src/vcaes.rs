use std::time::{Duration, Instant};

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

pub struct VCAESScheduler {
    n_schedules: usize,
    memory_length: usize,
    rng: Pcg64,
    sbx_eta: f64,
    obj_eval_limit: i64,
    //time_limit: Duration,
}

impl VCAESScheduler {
    pub fn new(n_schedules: usize, memory_length: usize, sbx_eta: f64, seed: u64,
               obj_eval_limit: i64,
               //time_limit: Duration
               ) -> Self {
        Self {
            n_schedules,
            memory_length,
            rng: Pcg64::seed_from_u64(seed),
            sbx_eta,
            obj_eval_limit,
            //time_limit,
        }
    }

    pub fn from_params(params: &SchedulerParams) -> Self {
        Self {
            n_schedules: params.get::<usize, &str>("n_schedules").unwrap(),
            memory_length: params.get::<usize, &str>("memory_length").unwrap(),
            rng: Pcg64::seed_from_u64(params.get::<u64, &str>("seed").unwrap()),
            sbx_eta: params.get::<f64, &str>("sbx_eta").unwrap(),
            obj_eval_limit: 0,
            //time_limit: Duration::from_secs_f64(params.get::<f64, &str>("time_limit").unwrap()),
        }
    }

    fn schedule(&mut self, dag: &DAG, system: System, config: Config, ctx: &SimulationContext) -> Vec<Vec<Action>> {
        let start = Instant::now();
        let mut schedules = vec![Schedule::new(dag.get_tasks().len()); self.n_schedules];
        for i in 0..self.n_schedules {
            for j in 0..schedules[i].assignment.len() {
                loop {
                    schedules[i].assignment[j] = self.rng.gen_range(0..system.resources.len());
                    if dag.get_task(j).is_allowed_on(schedules[i].assignment[j]) {
                        break;
                    }
                }
            }
        }
        for schedule in &mut schedules {
            self.obj_eval_limit -= 1;
            schedule.compute_objectives(dag, &system, &config, ctx);
        }
        let n_tasks = dag.get_tasks().len();
        let mut matr = vec![vec![0f64; n_tasks]; self.memory_length];
        let mut refvec = vec![[0f64; 2]; self.n_schedules];
        let refstep = 1.0 / ((refvec.len() as f64) - 1.);
        refvec[0][0] = 1.;
        for i in 1..refvec.len() {
            refvec[i][0] = refvec[i - 1][0] - refstep;
            refvec[i][1] = refvec[i - 1][1] + refstep;
        }
        let avg_net_time = system.avg_net_time(ctx.id(), &config.data_transfer_mode);

        let task_count = dag.get_tasks().len();

        let task_ranks = calc_ranks(system.avg_flop_time(), avg_net_time, dag);
        let mut task_ids = (0..task_count).collect::<Vec<_>>();
        task_ids.sort_by(|&a, &b| task_ranks[b].total_cmp(&task_ranks[a]));
        let all_indices = task_ids; //(0..n_tasks).collect::<Vec<usize>>();
        let generations = 5;
        while self.obj_eval_limit > 0 {
        //while start.elapsed() < self.time_limit {
            let k = self.adaptive_coevolution(&mut schedules, &matr, &refvec, generations, dag, &system, &config, ctx);
            for j in 0..self.memory_length - 1 {
                for i in 0..n_tasks {
                    matr[j][i] = matr[j + 1][i];
                }
            }
            for i in 0..n_tasks {
                matr[self.memory_length - 1][i] = k[i];
            }
            let mut new_schedules = self.evolve2(&schedules, &all_indices, dag, &system, &config, ctx);
            for s in &schedules {
                new_schedules.push(s.clone());
            }
            schedules = self.select_population(new_schedules);
        }
        println!("VCAES elapsed secs = {:?}", start.elapsed());
        schedules
            .into_iter()
            .map(|x| x.to_actions(dag, &system, &config, ctx))
            .collect::<Vec<_>>()
    }

    fn adaptive_coevolution(
        &mut self,
        schedules: &mut Vec<Schedule>,
        matr: &[Vec<f64>],
        refvec: &[[f64; 2]],
        base_generations: usize,
        dag: &DAG,
        system: &System,
        config: &Config,
        ctx: &SimulationContext,
    ) -> Vec<f64> {
        let mut h = vec![0f64; matr[0].len()];
        let mut hsum = 0f64;
        for i in 0..h.len() {
            for row in matr {
                h[i] += row[i];
            }
            hsum += h[i];
        }
        let mut order = vec![0; h.len()];
        for i in 0..h.len() {
            order[i] = i;
        }
        order.sort_by(|i, j| h[*i].total_cmp(&h[*j]).reverse());
        let mut k = vec![0f64; h.len()];
        let group_size = 10;
        let mut ptr = 0;
        while ptr < order.len() {
            let r = order.len().min(ptr + group_size);
            let mut gsum = 0f64;
            let mut group = order[ptr..r].to_vec();
            for i in ptr..r {
                gsum += h[order[i]];
            }
            let generations = ((base_generations as f64) * gsum / hsum).ceil() as usize;
            let old = schedules.clone();
            for _ in 0..generations {
                let mut new_schedules = self.evolve2(&schedules, &group, dag, system, config, ctx);
                for s in schedules.iter() {
                    new_schedules.push(s.clone());
                }
                *schedules = self.select_population(new_schedules);
            }
            let mut old_assoc = vec![0; refvec.len()];
            for i in 0..refvec.len() {
                for j in 1..old.len() {
                    let dot = old[j].makespan * refvec[i][0] + old[j].cost * refvec[i][1];
                    let pdot = old[old_assoc[i]].makespan * refvec[i][0] + old[old_assoc[i]].cost * refvec[i][1];
                    if pdot > dot {
                        old_assoc[i] = j;
                    }
                }
            }
            for i in 0..schedules.len() {
                let mut assoc = 0;
                for j in 1..refvec.len() {
                    let dot = schedules[i].makespan * refvec[j][0] + schedules[i].cost * refvec[j][1];
                    let pdot = schedules[i].makespan * refvec[assoc][0] + schedules[i].cost * refvec[assoc][1];
                    if pdot > dot {
                        assoc = j;
                    }
                }
                let p = old_assoc[assoc];
                let pfit = old[p].makespan * refvec[assoc][0]
                    + old[p].cost * refvec[assoc][1]
                    + (old[p].makespan * refvec[assoc][1] - old[p].cost * refvec[assoc][0]).abs();
                let qfit = schedules[i].makespan * refvec[assoc][0]
                    + schedules[i].cost * refvec[assoc][1]
                    + (schedules[i].makespan * refvec[assoc][1] - schedules[i].cost * refvec[assoc][0]).abs();
                for j in ptr..r {
                    k[order[j]] += 0f64.max(pfit - qfit) / (generations as f64);
                }
            }
            ptr = r;
        }
        k
    }

    fn evolve2(
        &mut self,
        schedules: &[Schedule],
        limit: &[usize],
        dag: &DAG,
        system: &System,
        config: &Config,
        ctx: &SimulationContext,
    ) -> Vec<Schedule> {
        let mut parents = Vec::with_capacity(self.n_schedules);
        let mut perm = (0..self.n_schedules).collect::<Vec<_>>();
        perm.shuffle(&mut self.rng);
        for i in 0..self.n_schedules {
            let j = (i + 1) % self.n_schedules;
            parents.push(perm[i].min(perm[j]));
        }
        parents.shuffle(&mut self.rng);
        let mutation_rate = 1. / (limit.len() as f64);
        let mut result = Vec::new();
        let n_tasks = dag.get_tasks().len();
        let n_resources = system.resources.len();
        for w in parents.chunks_exact(2) {
            let i = w[0];
            let j = w[1];
            if i == j {
                continue;
            }
            /*for _ in 0..(self.n_schedules + 1) / 2 {
            let i = self.rng.gen_range(0..schedules.len());
            let mut j = i;
            while j == i {
                j = self.rng.gen_range(0..schedules.len());
            }*/
            let mut a = schedules[i].clone();
            let mut b = schedules[j].clone();
            for k in 0..limit.len() {
                let task_id = limit[k];
                let task = dag.get_task(task_id);
                let mut allowed = Vec::new();
                for i in 0..n_resources {
                    if task.is_allowed_on(i) {
                        allowed.push(i);
                    }
                }
                if self.rng.gen_range(0f64..1f64) <= 1. {
                    //0.5 {
                    let mut y1 = allowed
                        .iter()
                        .position(|x| *x == schedules[i].assignment[task_id])
                        .unwrap() as f64;
                    let mut y2 = allowed
                        .iter()
                        .position(|x| *x == schedules[j].assignment[task_id])
                        .unwrap() as f64;
                    if y1 == y2 {
                        continue;
                    }
                    if y1 > y2 {
                        (y1, y2) = (y2, y1);
                    }
                    let lb = 0f64;
                    let ub = (allowed.len() - 1) as f64;
                    let u = self.rng.gen_range(0f64..1f64);
                    let mut beta = 1. + 2. * (y1 - lb) / (y2 - y1);
                    let mut alpha = 2. - beta.powf(-self.sbx_eta - 1.);
                    let mut betaq = if u <= 1. / alpha {
                        (u * alpha).powf(1. / (self.sbx_eta + 1.))
                    } else {
                        (1. / (2. - u * alpha)).powf(1. / (self.sbx_eta + 1.))
                    };
                    let mut var = 0.5 * (y1 + y2 - betaq * (y2 - y1));
                    /*if self.rng.gen_range(0f64..1f64) < mutation_rate {
                        let u = self.rng.gen_range(0f64..1f64);
                        var = if u <= 0.5 {
                            var + ((u * 2.).powf(1. / (1. + self.mutation_eta)) - 1.) * var
                        } else {
                            var + (1. - (2. - u * 2.).powf(1. / (1. + self.mutation_eta))) * (((allowed.len() - 1) as f64) - var)
                        };
                    }*/
                    a.assignment[task_id] = allowed[var.round() as usize];
                    beta = 1. + 2. * (ub - y2) / (y2 - y1);
                    alpha = 2. - beta.powf(-self.sbx_eta - 1.);
                    betaq = if u <= 1. / alpha {
                        (u * alpha).powf(1. / (self.sbx_eta + 1.))
                    } else {
                        (1. / (2. - u * alpha)).powf(1. / (self.sbx_eta + 1.))
                    };
                    var = 0.5 * (y1 + y2 + betaq * (y2 - y1));
                    /*if self.rng.gen_range(0f64..1f64) < mutation_rate {
                        let u = self.rng.gen_range(0f64..1f64);
                        var = if u <= 0.5 {
                            var + ((u * 2.).powf(1. / (1. + self.mutation_eta)) - 1.) * var
                        } else {
                            var + (1. - (2. - u * 2.).powf(1. / (1. + self.mutation_eta))) * (((allowed.len() - 1) as f64) - var)
                        };
                    }*/
                    b.assignment[task_id] = allowed[var.round() as usize];
                }
            }
            for k in 0..limit.len() {
                if self.rng.gen_range(0f64..1f64) < mutation_rate {
                    loop {
                        a.assignment[limit[k]] = self.rng.gen_range(0..system.resources.len());
                        if dag.get_task(limit[k]).is_allowed_on(a.assignment[limit[k]]) {
                            break;
                        }
                    }
                }
            }
            for k in 0..limit.len() {
                if self.rng.gen_range(0f64..1f64) < mutation_rate {
                    loop {
                        b.assignment[limit[k]] = self.rng.gen_range(0..system.resources.len());
                        if dag.get_task(limit[k]).is_allowed_on(b.assignment[limit[k]]) {
                            break;
                        }
                    }
                }
            }
            /*for k in 0..limit.len() {
                let task_id = limit[k];
                let task = dag.get_task(task_id);
                let mut allowed = Vec::new();
                for i in 0..n_resources {
                    if task.is_allowed_on(i) {
                        allowed.push(i);
                    }
                }
                if self.rng.gen_range(0f64..1f64) < mutation_rate {
                    let init = allowed.iter().position(|x| *x == a.assignment[task_id]).unwrap() as f64;
                    let u = self.rng.gen_range(0f64..1f64);
                    let var = if u <= 0.5 {
                        init + ((u * 2.).powf(1. / (1. + self.mutation_eta)) - 1.) * init
                    } else {
                        init + (1. - (2. - u * 2.).powf(1. / (1. + self.mutation_eta))) * (((allowed.len() - 1) as f64) - init)
                    };
                    a.assignment[limit[k]] = allowed[var.round() as usize];
                }
                if self.rng.gen_range(0f64..1f64) < mutation_rate {
                    let init = allowed.iter().position(|x| *x == b.assignment[task_id]).unwrap() as f64;
                    let u = self.rng.gen_range(0f64..1f64);
                    let var = if u <= 0.5 {
                        init + ((u * 2.).powf(1. / (1. + self.mutation_eta)) - 1.) * init
                    } else {
                        init + (1. - (2. - u * 2.).powf(1. / (1. + self.mutation_eta))) * (((allowed.len() - 1) as f64) - init)
                    };
                    b.assignment[limit[k]] = allowed[var.round() as usize];
                }
            }*/
            result.push(a);
            result.push(b);
        }
        for s in &mut result {
            self.obj_eval_limit -= 1;
            s.compute_objectives(dag, system, config, ctx);
        }
        result
    }

    fn evolve(
        &mut self,
        schedules: &[Schedule],
        limit: &[usize],
        dag: &DAG,
        system: &System,
        config: &Config,
        ctx: &SimulationContext,
    ) -> Vec<Schedule> {
        let mut result = Vec::new();
        for _ in 0..(self.n_schedules + 1) / 2 {
            let i = self.rng.gen_range(0..schedules.len());
            let mut j = i;
            while j == i {
                j = self.rng.gen_range(0..schedules.len());
            }
            //let border = limit.len() - 1;
            let border = self.rng.gen_range(0..limit.len());
            let mut a = schedules[i].clone();
            let mut b = schedules[j].clone();
            for k in 0..border + 1 {
                /*if self.rng.gen_range(0..2) == 1 {
                    std::mem::swap(&mut a.assignment[limit[k]], &mut b.assignment[limit[k]]);
                }*/
                std::mem::swap(&mut a.assignment[limit[k]], &mut b.assignment[limit[k]]);
            }
            for k in 0..limit.len() {
                if self.rng.gen_range(0f64..1f64) < 0.05 {
                    loop {
                        a.assignment[limit[k]] = self.rng.gen_range(0..system.resources.len());
                        if dag.get_task(limit[k]).is_allowed_on(a.assignment[limit[k]]) {
                            break;
                        }
                    }
                }
            }
            for k in 0..limit.len() {
                if self.rng.gen_range(0f64..1f64) < 0.05 {
                    loop {
                        b.assignment[limit[k]] = self.rng.gen_range(0..system.resources.len());
                        if dag.get_task(limit[k]).is_allowed_on(b.assignment[limit[k]]) {
                            break;
                        }
                    }
                }
            }
            result.push(a);
            result.push(b);
        }
        for s in &mut result {
            self.obj_eval_limit -= 1;
            s.compute_objectives(dag, system, config, ctx);
        }
        result
    }

    fn select_population(&self, old: Vec<Schedule>) -> Vec<Schedule> {
        let mut f = fast_non_dominated_sort(&old);
        let mut rank = vec![0; old.len()];
        for i in 0..f.len() {
            for j in f[i].iter().copied() {
                rank[j] = i;
            }
        }
        let mut crowding = vec![0f64; old.len()];
        let mut new = Vec::new();
        let mut ptr = 0;
        while new.len() + f[ptr].len() < self.n_schedules {
            crowding_distance_assignment(&old, f[ptr].clone(), &mut crowding);
            f[ptr].sort_by(|i, j| crowding[*j].total_cmp(&crowding[*i]));
            for i in f[ptr].iter().copied() {
                new.push(old[i].clone());
            }
            ptr += 1;
        }
        let l = f[ptr].len();
        crowding_distance_assignment(&old, f[ptr].clone(), &mut crowding);
        f[ptr].sort_by(|a, b| rank[*a].cmp(&rank[*b]).then(crowding[*b].total_cmp(&crowding[*a])));
        let remain = self.n_schedules - new.len();
        for i in 0..remain {
            new.push(old[f[ptr][i]].clone());
        }
        new
    }
}

fn crowding_distance_assignment(s: &[Schedule], mut seq: Vec<usize>, out: &mut [f64]) {
    for obj in 0..2 {
        let fmax = s[seq
            .iter()
            .copied()
            .max_by(|x, y| s[*x].obj()[obj].total_cmp(&s[*y].obj()[obj]))
            .unwrap()]
        .obj()[obj];
        let fmin = s[seq
            .iter()
            .copied()
            .min_by(|x, y| s[*x].obj()[obj].total_cmp(&s[*y].obj()[obj]))
            .unwrap()]
        .obj()[obj];
        seq.sort_by(|a, b| s[*a].obj()[obj].total_cmp(&s[*b].obj()[obj]));
        out[seq[0]] = f64::INFINITY;
        out[*seq.last().unwrap()] = f64::INFINITY;
        for i in 1..seq.len() - 1 {
            out[seq[i]] += (s[seq[i + 1]].obj()[obj] - s[seq[i - 1]].obj()[obj]) / (fmax - fmin);
        }
    }
}

fn dominates(obj1: [f64; 2], obj2: [f64; 2]) -> bool {
    (obj1[0] <= obj2[0] && obj1[1] < obj2[1]) || (obj1[0] < obj2[0] && obj1[1] <= obj2[1])
}

fn fast_non_dominated_sort(schedules: &[Schedule]) -> Vec<Vec<usize>> {
    let mut fronts = Vec::new();
    let mut s_dom = vec![Vec::new(); schedules.len()];
    let mut ctr = vec![0; schedules.len()];
    let mut front = Vec::new();
    for (i, p) in schedules.iter().enumerate() {
        for (j, q) in schedules.iter().enumerate() {
            if dominates(p.obj(), q.obj()) {
                s_dom[i].push(j);
            } else if dominates(q.obj(), p.obj()) {
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

#[derive(Clone)]
struct Schedule {
    pub assignment: Vec<usize>,
    pub cost: f64,
    pub makespan: f64,
}

impl Schedule {
    pub fn new(size: usize) -> Self {
        Self {
            assignment: vec![0; size],
            cost: 0.,
            makespan: 0.,
        }
    }

    pub fn compute_objectives(&mut self, dag: &DAG, system: &System, config: &Config, ctx: &SimulationContext) {
        let avg_net_time = system.avg_net_time(ctx.id(), &config.data_transfer_mode);

        let task_count = dag.get_tasks().len();

        let task_ranks = calc_ranks(system.avg_flop_time(), avg_net_time, dag);
        let mut task_ids = (0..task_count).collect::<Vec<_>>();
        task_ids.sort_by(|&a, &b| task_ranks[b].total_cmp(&task_ranks[a]));
        let mut tmp = PartialSchedule::new(dag, DataTransferStrategy::Eager, system, config, ctx);
        for id in task_ids.into_iter() {
            tmp.assign_task(id, self.assignment[id]);
        }
        self.makespan = tmp.makespan;
        self.cost = tmp.cost;
    }

    pub fn obj(&self) -> [f64; 2] {
        [self.makespan, self.cost]
    }

    pub fn to_actions(&self, dag: &DAG, system: &System, config: &Config, ctx: &SimulationContext) -> Vec<Action> {
        let avg_net_time = system.avg_net_time(ctx.id(), &config.data_transfer_mode);

        let task_count = dag.get_tasks().len();

        let task_ranks = calc_ranks(system.avg_flop_time(), avg_net_time, dag);
        let mut task_ids = (0..task_count).collect::<Vec<_>>();
        task_ids.sort_by(|&a, &b| task_ranks[b].total_cmp(&task_ranks[a]));
        let mut tmp = PartialSchedule::new(dag, DataTransferStrategy::Eager, system, config, ctx);
        for id in task_ids.into_iter() {
            tmp.assign_task(id, self.assignment[id]);
        }
        tmp.actions.into_iter().map(|x| x.1).collect::<Vec<_>>()
    }
}

impl ParetoScheduler for VCAESScheduler {
    fn find_pareto_front(
        &mut self,
        dag: &DAG,
        system: System,
        config: Config,
        ctx: &SimulationContext,
    ) -> Vec<Vec<Action>> {
        assert_eq!(
            config.data_transfer_mode,
            DataTransferMode::Direct,
            "VCAESScheduler only supports DataTransferMode::Direct"
        );

        if dag.get_tasks().iter().any(|task| task.min_cores != task.max_cores) {
            log_warn!(
                ctx,
                "some tasks support different number of cores, but VCAES will always use min_cores"
            );
        }

        self.schedule(dag, system, config, ctx)
    }
}
