use std::collections::BTreeSet;

use indexmap::{IndexMap, IndexSet};
use rand_pcg::Pcg64;

use crate::ls::common::*;

/// Unlike other local search components, the generator doesn't have a reset method.
/// However, it should be reentrable.
pub trait InitialSolutionGenerator {
    fn generate(&mut self, instance: &Instance, rng: &mut Pcg64) -> State;
}

/// This generator should only be used when local search ALWAYS gets initial solution passed to
/// run method (this is the case for nested search in ILS). Otherwise the search will be invalid.
pub struct StubInitialSolutionGenerator {}

impl InitialSolutionGenerator for StubInitialSolutionGenerator {
    fn generate(&mut self, _instance: &Instance, _rng: &mut Pcg64) -> State {
        Default::default()
    }
}

pub struct GreedyInitialSolutionGenerator {}

impl InitialSolutionGenerator for GreedyInitialSolutionGenerator {
    fn generate(&mut self, instance: &Instance, _rng: &mut Pcg64) -> State {
        let w = instance.keepalive;
        let mut s: State = Default::default();
        let n = instance.req_app.len();
        let mut cont = BTreeSet::new();
        let mut alive = IndexMap::<usize, IndexSet<usize>>::new();
        let mut alive_by_host = IndexMap::<usize, IndexSet<usize>>::new();
        for i in 0..n {
            let app = instance.req_app[i];
            let t = instance.req_start[i];
            let dur = instance.req_dur[i];
            while !cont.is_empty() {
                let it: (u64, usize) = *cont.iter().next().unwrap();
                if it.0 < t {
                    let f = s.containers[it.1].app;
                    alive.get_mut(&f).unwrap().remove(&it.1);
                    alive_by_host
                        .get_mut(&s.containers[it.1].host)
                        .unwrap()
                        .remove(&it.1);
                    cont.remove(&it);
                } else {
                    break;
                }
            }
            let mut placed = false;
            let set = alive.entry(app).or_insert(IndexSet::<usize>::new());
            for id in set.iter().copied() {
                if s.containers[id].end - w < t && s.containers[id].end >= t {
                    cont.remove(&(s.containers[id].end, id));
                    s.containers[id].end = t + dur + w;
                    s.containers[id].invocations.push(i);
                    cont.insert((s.containers[id].end, id));
                    placed = true;
                    break;
                }
            }
            if placed {
                continue;
            }
            let mut chosen = 0;
            let mut best_t = u64::MAX;
            for i in 0..instance.hosts.len() {
                let mut can_start = vec![t; instance.hosts[i].len()];
                let mut curr = alive_by_host
                    .entry(i)
                    .or_default()
                    .iter()
                    .map(|x| (s.containers[*x].end, *x))
                    .collect::<Vec<_>>();
                curr.sort();
                for r in 0..can_start.len() {
                    let mut sum: u64 = curr.iter().map(|x| s.containers[x.1].resources[r]).sum();
                    for (end, x) in curr.iter().copied() {
                        if sum + instance.apps[app][r] <= instance.hosts[i][r] {
                            break;
                        }
                        sum -= instance.apps[s.containers[x].app][r];
                        can_start[r] = end;
                    }
                }
                let start = can_start
                    .iter()
                    .copied()
                    .fold(u64::MIN, |acc, x| if x > acc { x } else { acc });
                if start < best_t {
                    best_t = start;
                    chosen = i;
                }
            }
            let c = Container {
                host: chosen,
                app,
                invocations: vec![i],
                resources: instance.apps[app].clone(),
                start: best_t,
                end: best_t + dur + w + instance.app_coldstart[app],
            };
            let id = s.containers.len();
            alive_by_host.entry(chosen).or_default().insert(id);
            alive.entry(app).or_default().insert(id);
            cont.insert((c.end, id));
            s.containers.push(c);
        }
        s.recompute_objective(instance);
        println!("init objective = {}", s.objective);
        match s.validate(instance) {
            Ok(_) => {}
            Err(e) => {
                panic!("{}", e);
            }
        }
        s
    }
}
