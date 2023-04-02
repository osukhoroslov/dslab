use std::collections::HashMap;

use dslab_faas::config::Config;
use dslab_faas::trace::Trace;

use crate::common::Instance;
use crate::matching::*;
use crate::estimator::{Estimation, Estimator};

fn path_cover_single(n: usize, edges: Vec<(usize, usize)>, kind: Vec<u8>) -> usize {
    let mat = convex_matching(n, edges.clone());
    let result = mat.iter().filter(|&x| *x == usize::MAX).count();
    let class = classify_edges(n, edges.clone(), mat.clone());
    let mut inc = 0;
    let mut in_edges = vec![Vec::new(); n];
    let mut first = vec![usize::MAX; n];
    for i in 0..n {
        if mat[i] != usize::MAX {
            first[mat[i]] = i;
        }
    }
    for (i, (u, v)) in edges.iter().copied().enumerate() {
        in_edges[v].push(i);
    }
    for (i, (u, v)) in edges.iter().copied().enumerate() {
        if mat[u] == v && kind[i] == 1 && first[u] == usize::MAX {
            let mut ok = class[i] == MatchingEdge::InAll;
            for j in in_edges[v].iter().copied() {
                if i != j {
                    ok &= class[j] == MatchingEdge::InNone;
                }
            }
            if ok {
                inc = 1;
                break;
            }
        } else if mat[u] == v && kind[i] == 2 && first[u] != usize::MAX {
            let mut ok = class[i] == MatchingEdge::InAll;
            for j in in_edges[u].iter().copied() {
                if edges[j].0 == first[u] {
                    ok &= class[j] == MatchingEdge::InAll;
                }
            }
            if ok {
                inc = 1;
                break;
            }
        }
    }
    if inc == 1 {
        println!("INC!");
    }
    result + inc
}

fn path_cover(instance: &Instance) -> u64 {
    let mut result = 0u64;
    let mut app_invs = vec![Vec::<usize>::new(); instance.apps.len()];
    for (i, app) in instance.req_app.iter().enumerate() {
        app_invs[*app].push(i);
    }
    for (app, invs) in app_invs.drain(..).enumerate() {
        let mut edges = Vec::new();
        let mut kind = Vec::<u8>::new();
        for ii in 0..invs.len() {
            let i = invs[ii];
            let t = instance.req_start[i];
            for jj in 0..ii {
                let j = invs[jj];
                let mut ok1 = instance.req_start[j] + instance.req_dur[j] <= t  && instance.req_start[j] + instance.req_dur[j] + instance.keepalive >= t;
                let mut ok2 = instance.req_start[j] + instance.req_dur[j] + instance.app_coldstart[app] <= t  && instance.req_start[j] + instance.req_dur[j] + instance.app_coldstart[app] + instance.keepalive >= t;
                if ok1 || ok2 {
                    edges.push((jj, ii));
                    let mut k = 0;
                    if ok1 {
                        k = k | 1;
                    }
                    if ok2 {
                        k = k | 2;
                    }
                    kind.push(k);
                }
            }
        }
        result += instance.app_coldstart[app] * (path_cover_single(invs.len(), edges, kind) as u64);
    }
    result
}

pub struct PathCoverEstimator {
    keepalive: f64,
    round_mul: f64,
}

impl PathCoverEstimator {
    pub fn new(keepalive: f64, round_mul: f64) -> Self {
        Self {
            keepalive,
            round_mul,
        }
    }
}

impl Estimator for PathCoverEstimator {
    type EstimationType = f64;

    fn estimate(&mut self, config: &Config, trace: &dyn Trace) -> Estimation<Self::EstimationType> {
        let mut instance: Instance = Default::default();
        instance.keepalive = (self.keepalive * self.round_mul).round() as u64;
        let mut resource_map = HashMap::<String, usize>::new();
        for host in config.hosts.iter() {
            for item in host.resources.iter() {
                let name = &item.0;
                let new_id = resource_map.len();
                resource_map.entry(name.clone()).or_insert(new_id);
            }
        }
        let n_hosts = config.hosts.len();
        instance.hosts = vec![vec![0u64; resource_map.len()]; n_hosts];
        for (i, host) in config.hosts.iter().enumerate() {
            for item in host.resources.iter() {
                let id = resource_map.get(&item.0).unwrap();
                instance.hosts[i][*id] = item.1;
            }
        }
        instance.apps = Vec::new();
        instance.app_coldstart = Vec::new();
        for item in trace.app_iter() {
            instance.app_coldstart.push((item.container_deployment_time * self.round_mul).round() as u64);
            instance.apps.push(vec![0u64; resource_map.len()]);
            for res in item.container_resources.iter() {
                let it = resource_map.get(&res.0);
                assert!(
                    it.is_some(),
                    "Some application has resource that is not present on hosts."
                );
                let id = instance.apps.len() - 1;
                instance.apps[id][*it.unwrap()] = res.1;
            }
        }
        let func = trace.function_iter().map(|x| x as usize).collect::<Vec<usize>>();
        instance.req_app = Vec::new();
        instance.req_dur = Vec::new();
        instance.req_start = Vec::new();
        let mut raw_items = Vec::<(u64, u64, usize)>::new();
        for item in trace.request_iter() {
            raw_items.push(((item.time * self.round_mul).round() as u64, (item.duration * self.round_mul).round() as u64, item.id as usize));
        }
        raw_items.sort();
        for item in raw_items.drain(..) {
            instance.req_app.push(func[item.2]);
            instance.req_dur.push(item.1);
            instance.req_start.push(item.0);
        }
        let obj = path_cover(&instance);
        Estimation::LowerBound((obj as f64) / self.round_mul)
    }
}
