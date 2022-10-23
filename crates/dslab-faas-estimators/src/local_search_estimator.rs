use std::boxed::Box;
use std::collections::HashMap;

use dslab_faas::config::Config;
use dslab_faas::trace::Trace;

use crate::estimator::{Estimation, Estimator};
use crate::ls::common::{Instance, OptimizationGoal};
use crate::ls::local_search::LocalSearch;

pub struct LocalSearchEstimator {
    goal: OptimizationGoal,
    search: LocalSearch,
    keepalive: f64,
}

impl LocalSearchEstimator {
    pub fn new(goal: OptimizationGoal, search: LocalSearch, keepalive: f64) -> Self {
        Self {
            goal,
            search,
            keepalive,
        }
    }
}

impl Estimator for LocalSearchEstimator {
    type EstimationType = f64;

    fn estimate(&mut self, config: Config, trace: Box<dyn Trace>) -> Estimation<Self::EstimationType> {
        let mut instance: Instance = Default::default();
        instance.keepalive = self.keepalive;
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
            instance.app_coldstart.push(item.container_deployment_time);
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
        let mut raw_items = Vec::<(f64, f64, usize)>::new();
        for item in trace.request_iter() {
            raw_items.push((item.time, item.duration, item.id as usize));
        }
        raw_items.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.2.cmp(&b.2)));
        for item in raw_items.drain(..) {
            instance.req_app.push(func[item.2]);
            instance.req_dur.push(item.1);
            instance.req_start.push(item.0);
        }
        let obj = self.search.run(&instance, None).objective;
        if self.goal == OptimizationGoal::Maximization {
            Estimation::LowerBound(obj)
        } else {
            Estimation::UpperBound(obj)
        }
    }
}
