use std::collections::BTreeSet;

use indexmap::{IndexMap, IndexSet};

use crate::common::Instance;

#[derive(Clone, Default)]
pub struct Container {
    pub host: usize,
    pub app: usize,
    pub invocations: Vec<usize>,
    pub resources: Vec<u64>,
    pub start: u64,
    pub end: u64,
}

pub fn generate_initial(instance: &Instance) -> (Vec<usize>, u64) {
    let w = instance.keepalive;
    let mut containers = Vec::<Container>::new();
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
                let f = containers[it.1].app;
                alive.get_mut(&f).unwrap().remove(&it.1);
                alive_by_host
                    .get_mut(&containers[it.1].host)
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
            if containers[id].end - w < t && containers[id].end >= t {
                cont.remove(&(containers[id].end, id));
                containers[id].end = t + dur + w;
                containers[id].invocations.push(i);
                cont.insert((containers[id].end, id));
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
                .map(|x| (containers[*x].end, *x))
                .collect::<Vec<_>>();
            curr.sort();
            for r in 0..can_start.len() {
                let mut sum: u64 = curr.iter().map(|x| containers[x.1].resources[r]).sum();
                for (end, x) in curr.iter().copied() {
                    if sum + instance.apps[app][r] <= instance.hosts[i][r] {
                        break;
                    }
                    sum -= instance.apps[containers[x].app][r];
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
        let id = containers.len();
        alive_by_host.entry(chosen).or_default().insert(id);
        alive.entry(app).or_default().insert(id);
        cont.insert((c.end, id));
        containers.push(c);
    }
    let mut objective = 0u64;
    let mut result = vec![usize::MAX; instance.req_app.len()];
    for c in containers.iter() {
        let f = instance.req_app[c.invocations[0]];
        for i in 1..c.invocations.len() {
            result[c.invocations[i]] = c.invocations[i - 1];
        }
        objective += instance.app_coldstart[f] + c.start - instance.req_start[c.invocations[0]];
    }
    println!("init objective = {}", objective);
    (result, objective)
}
