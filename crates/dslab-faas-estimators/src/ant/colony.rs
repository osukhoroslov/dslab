use std::collections::HashMap;

use rand::prelude::*;
use rand::distributions::WeightedIndex;
use rand_pcg::Pcg64;

use crate::common::Instance;
use crate::ant::initial::generate_initial;

const MISS_ADD: u64 = 100000000u64;

#[derive(Clone, Default)]
pub struct AntColony {
    n_ants: usize,
    evaporation: f64,
    alpha: f64,
    init_beta: f64,
    beta_decay: f64,
    n_iters: usize,
    max_iters_elitist: usize,
    seed: u64,
}

impl AntColony {
    pub fn new(seed: u64) -> Self {
        Self {
            n_ants: 50,
            evaporation: 0.05,
            alpha: 1.,
            init_beta: 5.,
            beta_decay: 0.05,
            n_iters: 100,
            max_iters_elitist: 10,
            seed,
        }
    }

    pub fn run(&mut self, instance: &Instance) -> u64 {
        let mut rng = Pcg64::seed_from_u64(self.seed);
        let mut possible_prev = vec![vec![usize::MAX]; instance.req_app.len()];
        let mut prev_kind = vec![vec![0u8]; instance.req_app.len()];
        let mut pheromone = vec![vec![1000.0f64]; instance.req_app.len()];
        let mut changed = vec![false; instance.req_app.len()];
        for i in 0..instance.req_app.len() {
            let app = instance.req_app[i];
            let t = instance.req_start[i];
            for j in 0..i {
                if instance.req_app[j] == app {
                    let mut ok1 = instance.req_start[j] + instance.req_dur[j] <= t  && instance.req_start[j] + instance.req_dur[j] + instance.keepalive >= t;
                    let mut ok2 = instance.req_start[j] + instance.req_dur[j] + instance.app_coldstart[app] <= t  && instance.req_start[j] + instance.req_dur[j] + instance.app_coldstart[app] + instance.keepalive >= t;
                    if ok1 != ok2 {
                        changed[j] = true;
                        possible_prev[i].push(j);
                        pheromone[i].push(1000.);
                        if ok1 {
                            prev_kind[i].push(1);
                        } else {
                            prev_kind[i].push(2);
                        }
                    } else if ok1 && ok2 {
                        possible_prev[i].push(j);
                        pheromone[i].push(1000.);
                        prev_kind[i].push(3);
                    }
                }
            }
        }
    
        for i in 0..changed.len() {
            if possible_prev[i].len() == 1 {
                changed[i] = false;
            }
        }

        let mut bad_cnt = HashMap::<usize, usize>::new();
        let mut total_cnt = HashMap::<usize, usize>::new();
        for (i, c) in changed.iter().copied().enumerate() {
            if c {
                *bad_cnt.entry(instance.req_app[i]).or_default() += 1;
            }
        }
        for app in instance.req_app.iter().copied() {
            *total_cnt.entry(app).or_default() += 1;
        }
        for (app, cnt) in bad_cnt.iter() {
            println!("bad = {} out of {}", *cnt, *total_cnt.get(app).unwrap());
        }
        let mut best_seq = vec![0; pheromone.len()];
        let mut best_obj = u64::MAX;
        let mut best_miss = usize::MAX;
        let mut global_best_obj = best_obj;
        /*let (mut best_seq, mut best_obj) = generate_initial(instance);
        let mut global_best_obj = best_obj;
        for i in 0..best_seq.len() {
            best_seq[i] = possible_prev[i].iter().position(|&x| x == best_seq[i]).unwrap();
        }*/
        let mut beta = self.init_beta;
        let mut unchanged = 0;
        for iter_id in 0..self.n_iters {
            let mut ants = Vec::with_capacity(self.n_ants);
            let mut ant_obj = vec![0u64; self.n_ants];
            let mut ant_miss = vec![0; self.n_ants];
            for ant in 0..self.n_ants {
                let mut seq = Vec::with_capacity(pheromone.len());
                let mut deleted = vec![false; pheromone.len()];
                for i in 0..pheromone.len() {
                    let mut probs = Vec::with_capacity(pheromone[i].len());
                    let mut possible = Vec::with_capacity(pheromone[i].len());
                    for j in 0..pheromone[i].len() {
                        if j > 0 && deleted[possible_prev[i][j]] {
                            continue;
                        }
                        if j > 0 && prev_kind[i][j] != 3 {
                            if prev_kind[i][j] == 1 && seq[possible_prev[i][j]] == 0{ 
                                continue;
                            }
                            if prev_kind[i][j] == 2 && seq[possible_prev[i][j]] != 0 {
                                continue;
                            }
                        }
                        let h: f64 = if possible_prev[i][j] == usize::MAX { 1. } else { 
                            //let d = if seq[possible_prev[i][j]] == 0 { instance.app_coldstart[instance.req_app[i]] } else { 0 };
                            //4. * (1. + 1. / ((instance.req_start[i] - instance.req_start[possible_prev[i][j]] - instance.req_dur[possible_prev[i][j]]) as f64)) 
                            4. * (1. + ((instance.req_start[i] - instance.req_start[possible_prev[i][j]] - instance.req_dur[possible_prev[i][j]]) as f64) / (instance.keepalive as f64)) 
                        };
                        let p = pheromone[i][j].powf(self.alpha) * h.powf(beta);
                        probs.push(p);
                        possible.push(j);
                    }
                    let choice = possible[WeightedIndex::new(&probs).unwrap().sample(&mut rng)];
                    seq.push(choice);
                    if choice != 0 {
                        deleted[possible_prev[i][choice]] = true;
                    }
                    if seq[i] == 0 {
                        ant_obj[ant] += instance.app_coldstart[instance.req_app[i]];
                    }
                }
                let mut cont_id: Vec<usize> = Vec::with_capacity(seq.len());
                let mut conts: Vec<Vec<usize>> = Vec::new();
                for i in 0..seq.len() {
                    if seq[i] == 0 {
                        cont_id.push(conts.len());
                        conts.push(vec![i]);
                    } else {
                        cont_id.push(cont_id[possible_prev[i][seq[i]]]);
                        conts[cont_id[i]].push(i);
                    }
                }
                println!("found {} containers", conts.len());
                let mut events = Vec::with_capacity(conts.len() * 2);
                for (i, cont) in conts.iter().enumerate() {
                    events.push((instance.req_start[cont[0]], 1, i));
                    let mut t = instance.req_start[cont[0]];
                    for id in cont.iter().copied() {
                        let mut d = instance.req_dur[id];
                        if id == cont[0] {
                            d += instance.app_coldstart[instance.req_app[id]];
                        }
                        assert!(t + instance.keepalive >= instance.req_start[id]);
                        assert!(t <= instance.req_start[id]);
                        t = t.max(instance.req_start[id]);
                        ant_obj[ant] += t - instance.req_start[id];
                        t += d;
                    }
                    t += instance.keepalive;
                    events.push((t, 0, i));
                }
                let mut used = vec![vec![0u64; instance.hosts[0].len()]; instance.hosts.len()];
                let mut loc = HashMap::<usize, usize>::new();
                let mut missed = 0;
                events.sort();
                for (_, kind, c) in events.drain(..) {
                    if kind == 0 {
                        if let Some(h) = loc.remove(&c) {
                            for r in 0..used[h].len() {
                                assert!(used[h][r] >= instance.apps[instance.req_app[conts[c][0]]][r]);
                                used[h][r] -= instance.apps[instance.req_app[conts[c][0]]][r];
                            }
                        }
                        continue;
                    }
                    let mut chosen = usize::MAX;
                    let mut util = -1.;
                    for h in 0..used.len() {
                        let mut ok = true;
                        let mut u = 0.;
                        for r in 0..used[h].len() {
                            if used[h][r] + instance.apps[instance.req_app[conts[c][0]]][r] > instance.hosts[h][r] {
                                ok = false;
                                break;
                            }
                            u += (used[h][r] as f64) / (instance.hosts[h][r] as f64);
                        }
                        if ok {
                            u /= used[h].len() as f64;
                            if u > util {
                                chosen = h;
                                util = u;
                            }
                        }
                    }
                    if chosen == usize::MAX {
                        missed += conts[c].len();
                        //ant_obj[ant] += 100000000u64 * (conts[c].len() as u64);
                    } else {
                        for r in 0..used[chosen].len() {
                            used[chosen][r] += instance.apps[instance.req_app[conts[c][0]]][r];
                        }
                        loc.insert(c, chosen);
                    }
                }
                ant_miss[ant] = missed;
                if missed > 0 {
                    ant_obj[ant] += MISS_ADD;
                    println!("missed {}", missed);
                }
                ants.push(seq);
            }
            for p in pheromone.iter_mut() {
                for x in p.iter_mut() {
                    *x *= 1. - self.evaporation;
                }
            }
            let mut ord = (0..self.n_ants).collect::<Vec<_>>();
            ord.sort_by_key(|i| (ant_miss[*i], ant_obj[*i]));
            println!("iter worst = {}; iter med = {}; iter best = {}", ant_obj[ord[self.n_ants - 1]], ant_obj[ord[self.n_ants/2]], ant_obj[ord[0]]);
            let mut coeff = 5000.;
            if (best_miss > ant_miss[ord[0]] || (best_miss == ant_miss[ord[0]] && ant_obj[ord[0]] < best_obj)) || unchanged == self.max_iters_elitist {
                best_seq = ants[ord[0]].clone();
                best_obj = ant_obj[ord[0]];
                best_miss = ant_miss[ord[0]];
                global_best_obj = global_best_obj.min(best_obj);
                unchanged = 0;
            } else {
                unchanged += 1;
                let obj = if best_miss > 0 { best_obj - MISS_ADD } else { best_obj };
                for (i, s) in best_seq.iter().copied().enumerate() {
                    pheromone[i][s] += self.evaporation * coeff * (-(best_miss as f64)).exp() / (obj as f64);
                }
            }
            for j in 0..=(self.n_ants/10) {
                let obj = if ant_miss[ord[j]] > 0 { ant_obj[ord[j]] - MISS_ADD } else {ant_obj[ord[j]]};
                for (i, s) in ants[ord[j]].iter().copied().enumerate() {
                    pheromone[i][s] += self.evaporation * (coeff - (j as f64)) * (-(ant_miss[ord[j]] as f64)).exp() / (obj as f64);
                }
            }
            beta -= self.beta_decay;
            beta = beta.max(0.);
            println!("new beta = {}", beta);
        }
        global_best_obj
    }
}
