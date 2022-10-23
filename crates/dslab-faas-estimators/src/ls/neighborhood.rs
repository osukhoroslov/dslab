use std::collections::{BTreeSet, HashSet};

use rand::prelude::*;
use rand_pcg::Pcg64;

use crate::ls::common::*;

pub trait Neighborhood {
    fn step(&mut self, s: &State, instance: &Instance, rng: &mut Pcg64) -> State;
    /// resets the neighborhood before the next local search run
    fn reset(&mut self);
}

/// This is an implementation of a destroy-repair method for LNS
pub struct DestroyRepairNeighborhood {
    destroy_part: f64,
}

impl DestroyRepairNeighborhood {
    pub fn new(destroy_part: f64) -> Self {
        Self { destroy_part }
    }

    fn check_after_change(
        containers: &[Container],
        host_id: usize,
        host: &[u64],
        change: Option<(usize, f64, f64)>,
    ) -> bool {
        let ch = change.unwrap_or((usize::MAX, 0.0, 0.0));
        for r in 0..host.len() {
            let mut events = Vec::new();
            for (i, c) in containers.iter().enumerate() {
                if c.host == host_id {
                    if ch.0 == i {
                        events.push((ch.1, 1, c.resources[r]));
                        events.push((ch.2, -1, c.resources[r]));
                    } else {
                        events.push((c.start, 1, c.resources[r]));
                        events.push((c.end, -1, c.resources[r]));
                    }
                }
            }
            events.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
            let mut sum = 0i64;
            for (_, sgn, cnt) in events.drain(..) {
                sum += (cnt as i64) * (sgn as i64);
                if sum > (host[r] as i64) {
                    return false;
                }
            }
        }
        true
    }
}

pub fn sample(n: usize, k: usize, rng: &mut Pcg64) -> Vec<usize> {
    let mut s = BTreeSet::new();
    for i in 0..n {
        let var = rng.gen::<u64>();
        s.insert((var, i));
        if s.len() > k {
            let it = *s.iter().next_back().unwrap();
            s.remove(&it);
        }
    }
    let mut v = s.iter().map(|x| x.1).collect::<Vec<_>>();
    v.sort();
    v
}

impl Neighborhood for DestroyRepairNeighborhood {
    fn step(&mut self, s: &State, instance: &Instance, rng: &mut Pcg64) -> State {
        // destroy step: we remove |invocations| * destroy_part invocations from current state
        // possibly breaking some containers into several more containers
        // it can be proven that after breaking no constraints are violated
        let w = instance.keepalive;
        let mut init = s.containers.clone();
        let cnt = ((instance.req_app.len() as f64) * self.destroy_part).ceil() as usize;
        let mut del_vec = sample(instance.req_app.len(), cnt, rng);
        let del = HashSet::<usize>::from_iter(del_vec.iter().cloned());
        let mut conts = Vec::new();
        for c in init.drain(..) {
            let mut start = c.start;
            let mut end = start;
            let mut invs = Vec::new();
            for id in c.invocations.iter().copied() {
                if del.contains(&id) {
                    continue;
                } else {
                    if !invs.is_empty() && end + w + EPS < instance.req_start[id] {
                        let new = Container {
                            host: c.host,
                            app: c.app,
                            invocations: invs,
                            resources: c.resources.clone(),
                            start: start,
                            end: end + w,
                        };
                        conts.push(new);
                        invs = Vec::new();
                    }
                    if invs.is_empty() {
                        start = instance.req_start[id];
                        end = start + instance.req_dur[id] + instance.app_coldstart[instance.req_app[id]];
                    } else {
                        end = end.max(instance.req_start[id]) + instance.req_dur[id];
                    }
                    invs.push(id);
                }
            }
            if !invs.is_empty() {
                let new = Container {
                    host: c.host,
                    app: c.app,
                    invocations: invs,
                    resources: c.resources.clone(),
                    start: start,
                    end: end + w,
                };
                conts.push(new);
            }
        }
        // repair step: we greedily insert the removed invocations back into solution
        del_vec.shuffle(rng);
        for id in del_vec.drain(..) {
            let init_host = ((1000000007u64 * (id as u64) + 1) % (instance.hosts.len() as u64)) as usize;
            // TODO: loop over all hosts. It requires more careful implementation of objective
            // functions
            let mut host = init_host;
            let mut placed = false;
            loop {
                //TODO: faster implementation
                //TODO: implement "connection" moves when the invocation can merge two containers
                //into one
                let mut target = 0;
                let mut can_insert = false;
                for (i, c) in conts.iter().enumerate() {
                    if c.app == instance.req_app[id]
                        && c.end - w <= instance.req_start[id] + EPS
                        && c.end + EPS >= instance.req_start[id]
                    {
                        if Self::check_after_change(
                            &conts,
                            host,
                            &instance.hosts[host],
                            Some((i, c.start, instance.req_start[id] + instance.req_dur[id] + w)),
                        ) {
                            target = i;
                            can_insert = true;
                            break;
                        }
                    }
                }
                if can_insert {
                    conts[target].invocations.push(id);
                    conts[target].end = instance.req_start[id] + instance.req_dur[id] + w;
                    placed = true;
                    break;
                }
                let atleast =
                    instance.req_start[id] + instance.req_dur[id] + instance.app_coldstart[instance.req_app[id]];
                let mut end_delta = 0.0;
                for (i, c) in conts.iter().enumerate() {
                    if c.app == instance.req_app[id]
                        && instance.req_start[c.invocations[0]] >= atleast
                        && instance.req_start[c.invocations[0]] <= atleast + w
                    {
                        let mut end = instance.req_start[c.invocations[0]];
                        for id in c.invocations.iter().copied() {
                            end = end.max(instance.req_start[id]) + instance.req_dur[id];
                        }
                        end += w;
                        if Self::check_after_change(
                            &conts,
                            host,
                            &instance.hosts[host],
                            Some((i, instance.req_start[id], end)),
                        ) {
                            target = i;
                            can_insert = true;
                            end_delta = c.end - end;
                            break;
                        }
                    }
                }
                if can_insert {
                    conts[target].invocations.insert(0, id);
                    conts[target].start = instance.req_start[id];
                    conts[target].end -= end_delta;
                    placed = true;
                    break;
                }
                let mut tmp = conts.clone();
                let c = Container {
                    host,
                    app: instance.req_app[id],
                    invocations: vec![id],
                    resources: instance.apps[instance.req_app[id]].clone(),
                    start: instance.req_start[id],
                    end: instance.req_start[id]
                        + instance.app_coldstart[instance.req_app[id]]
                        + instance.req_dur[id]
                        + w,
                };
                tmp.push(c);
                if Self::check_after_change(&tmp, host, &instance.hosts[host], None) {
                    conts = tmp;
                    placed = true;
                    break;
                }
                host = (host + 1) % instance.hosts.len();
                if host == init_host {
                    break;
                }
            }
            assert!(placed);
        }
        let mut nxt = State {
            containers: conts,
            objective: 0.0,
        };
        nxt.recompute_objective(instance);
        println!("nxt obj = {}", nxt.objective);
        match nxt.validate(instance) {
            Ok(_) => {}
            Err(e) => {
                panic!("{}", e);
            }
        }
        nxt
    }

    fn reset(&mut self) {}
}
