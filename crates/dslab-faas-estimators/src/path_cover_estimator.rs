use std::collections::HashMap;

use dslab_faas::config::Config;
use dslab_faas::trace::Trace;

use crate::common::Instance;
use crate::estimator::{Estimation, Estimator};

fn path_cover_single(n: usize, edges: Vec<(usize, usize)>) -> usize {
    // TODO: improve the bound with tolerances
    let mut min = vec![n; n];
    let mut max = vec![0; n];
    let mut cnt = vec![0; n];
    let mut right = vec![Vec::<usize>::new(); n];
    for (u, v) in edges.iter().copied() {
        min[u] = min[u].min(v);
        max[u] = max[u].max(v);
        cnt[u] += 1;
        right[v].push(u);
    }
    // convexity check
    for i in 0..n {
        assert!(cnt[i] == 0 || max[i] + 1 - min[i] == cnt[i]);
    }
    let mut mat = vec![usize::MAX; n];
    for i in 0..n {
        let mut chosen = usize::MAX;
        for j in right[i].drain(..) {
            if mat[j] == usize::MAX && (chosen == usize::MAX || max[j] < max[chosen]) {
                chosen = j;
            }
        }
        if chosen != usize::MAX {
            mat[chosen] = i;
        }
    }
    mat.drain(..).filter(|&x| x == usize::MAX).count()
}

fn path_cover(instance: &Instance) -> u64 {
    let mut result = 0u64;
    let mut app_invs = vec![Vec::<usize>::new(); instance.apps.len()];
    for (i, app) in instance.req_app.iter().enumerate() {
        app_invs[*app].push(i);
    }
    for (app, invs) in app_invs.drain(..).enumerate() {
        let mut edges = Vec::new();
        for ii in 0..invs.len() {
            let i = invs[ii];
            let t = instance.req_start[i];
            for jj in 0..ii {
                let j = invs[jj];
                let mut ok1 = instance.req_start[j] + instance.req_dur[j] <= t  && instance.req_start[j] + instance.req_dur[j] + instance.keepalive >= t;
                let mut ok2 = instance.req_start[j] + instance.req_dur[j] + instance.app_coldstart[app] <= t  && instance.req_start[j] + instance.req_dur[j] + instance.app_coldstart[app] + instance.keepalive >= t;
                if ok1 || ok2 {
                    edges.push((jj, ii));
                }
            }
        }
        result += instance.app_coldstart[app] * (path_cover_single(invs.len(), edges) as u64);
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
