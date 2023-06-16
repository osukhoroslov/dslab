use std::collections::HashMap;

use dslab_faas::config::Config;
use dslab_faas::trace::Trace;

use indexmap::IndexSet;

use crate::common::Instance;
use crate::estimator::{Estimation, Estimator};
use crate::path_cover_estimator::path_cover;

#[cxx::bridge]
pub mod external {
    unsafe extern "C++" {
        include!("dslab-faas-estimators/include/multiknapsack.hpp");

        pub fn solve_multiknapsack(kind: &[u64], cost: &[u64], knapsacks: &[Vec<u64>], kinds: &[Vec<u64>]) -> u64;
    }
}

pub struct SegmentLowerEstimator {
    keepalive: f64,
    round_mul: f64,
}

impl SegmentLowerEstimator {
    pub fn new(keepalive: f64, round_mul: f64) -> Self {
        Self { keepalive, round_mul }
    }
}

impl Estimator for SegmentLowerEstimator {
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
            raw_items.push(((item.time * self.round_mul).round() as u64, (item.duration * self.round_mul).ceil() as u64, item.id as usize));
        }
        raw_items.sort();
        for item in raw_items.drain(..) {
            instance.req_app.push(func[item.2]);
            instance.req_dur.push(item.1);
            instance.req_start.push(item.0);
        }

        let n = instance.req_app.len();
        let mut nxt_app = vec![usize::MAX; instance.apps.len()];
        let mut nxt = vec![usize::MAX; n];
        for i in (0..n).rev() {
            nxt[i] = nxt_app[instance.req_app[i]];
            nxt_app[instance.req_app[i]] = i;
        }
        let mut events = Vec::with_capacity(2 * n);
        for i in 0..n {
            let mut delta = instance.keepalive;
            if nxt[i] != usize::MAX {
                delta = delta.min(instance.req_start[nxt[i]].max(instance.req_start[i] + instance.req_dur[i]) - instance.req_start[i] - instance.req_dur[i]);
            }
            delta = 0;
            events.push((instance.req_start[i], i, -1));
            events.push((instance.req_start[i] + instance.req_dur[i] + delta, i, 1));
        }
        events.sort();
        let mut segments = Vec::new();
        let mut leftmost = Vec::new();
        let mut rightmost = Vec::new();
        let mut bound = Vec::new();
        let mut active = IndexSet::new();
        let mut cover = vec![(usize::MAX, usize::MAX); n];
        let mut ptr = 0;
        let mut prv = u64::MAX;
        while ptr < n {
            let mut ptr2 = ptr;
            while ptr2 < n && events[ptr2].0 == events[ptr].0 {
                ptr2 += 1;
            }
            for evt in &events[ptr..ptr2] {
                if evt.2 == -1 {
                    cover[evt.1].0 = segments.len();
                    active.insert(evt.1);
                } else {
                    cover[evt.1].1 = segments.len() - 1;
                    let was = active.remove(&evt.1);
                    assert!(was);
                }
            }
            if prv != u64::MAX {
                let r = events[ptr].0;
                let l = prv;
                segments.push((l, r));
                leftmost.push(leftmost.len());
                rightmost.push(rightmost.len());
                let cost = active.iter().map(|&x| r - instance.req_start[x]).collect::<Vec<_>>();
                let kind = active.iter().map(|&x| instance.req_app[x] as u64).collect::<Vec<_>>();
                bound.push(external::solve_multiknapsack(&kind, &cost, &instance.hosts, &instance.apps));
            }
            prv = events[ptr].0;
            ptr = ptr2;
        }
        for i in 0..n {
            for j in cover[i].0..cover[i].1+1 {
                leftmost[j] = leftmost[j].min(cover[i].0);
                rightmost[j] = rightmost[j].max(cover[i].1);
            }
        }
        let mut dp = bound.clone();
        for i in 0..dp.len() {
            if i > 0 {
                dp[i] = dp[i].max(dp[i - 1]);
            }
            let j = leftmost[i];
            if j > 0 {
                dp[i] = dp[i].max(dp[j - 1] + bound[i]);
            }
        }

        Estimation::LowerBound((*dp.last().unwrap() as f64) / self.round_mul)
    }
}
