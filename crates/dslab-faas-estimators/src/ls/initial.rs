use std::cmp::Ordering;
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
        #[derive(Clone, Copy)]
        struct EndEvent {
            t: f64,
            id: usize,
        }

        impl PartialOrd for EndEvent {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }

        impl Ord for EndEvent {
            fn cmp(&self, other: &Self) -> Ordering {
                self.t.total_cmp(&other.t).then(self.id.cmp(&other.id))
            }
        }

        impl PartialEq for EndEvent {
            fn eq(&self, other: &Self) -> bool {
                self.t == other.t && self.id == other.id
            }
        }

        impl Eq for EndEvent {}

        let w = instance.keepalive;
        let mut s: State = Default::default();
        let n = instance.req_app.len();
        let mut cont = BTreeSet::<EndEvent>::new();
        let mut alive = IndexMap::<usize, IndexSet<usize>>::new();
        let mut alive_by_host = IndexMap::<(usize, usize), IndexSet<usize>>::new();
        for i in 0..n {
            let app = instance.req_app[i];
            let t = instance.req_start[i];
            let dur = instance.req_dur[i];
            while !cont.is_empty() {
                let it = *cont.iter().next().unwrap();
                if it.t < t {
                    let f = s.containers[it.id].app;
                    alive.get_mut(&f).unwrap().remove(&it.id);
                    alive_by_host
                        .get_mut(&(s.containers[it.id].host, f))
                        .unwrap()
                        .remove(&it.id);
                    cont.remove(&it);
                } else {
                    break;
                }
            }
            let mut placed = false;
            let set = alive.entry(app).or_insert(IndexSet::<usize>::new());
            for id in set.iter().copied() {
                if s.containers[id].end - w < t {
                    let item = EndEvent {
                        t: s.containers[id].end,
                        id,
                    };
                    cont.remove(&item);
                    s.containers[id].end = t + dur + w;
                    s.containers[id].invocations.push(i);
                    cont.insert(EndEvent {
                        t: s.containers[id].end,
                        id,
                    });
                    placed = true;
                    break;
                }
            }
            if placed {
                continue;
            }
            let mut chosen = 0;
            let mut best_t = f64::MAX;
            for i in 0..instance.hosts.len() {
                let mut can_start = vec![t; instance.hosts[i].len()];
                let mut curr = alive_by_host
                    .entry((i, app))
                    .or_default()
                    .iter()
                    .map(|x| (s.containers[*x].end, x))
                    .collect::<Vec<_>>();
                curr.sort_by(|a, b| a.0.total_cmp(&b.0));
                for r in 0..can_start.len() {
                    let mut s: u64 = curr.iter().map(|x| s.containers[*x.1].resources[r]).sum();
                    for (end, _) in curr.iter().copied() {
                        if s + instance.apps[app][r] <= instance.hosts[i][r] {
                            break;
                        }
                        s -= instance.apps[app][r];
                        can_start[r] = end;
                    }
                }
                let start = can_start
                    .iter()
                    .copied()
                    .fold(f64::MIN, |acc, x| if x > acc { x } else { acc });
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
            alive_by_host.entry((chosen, app)).or_default().insert(id);
            alive.entry(app).or_default().insert(id);
            cont.insert(EndEvent { t: c.end, id });
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
