use std::collections::HashMap;

use dslab_faas::config::Config;
use dslab_faas::trace::Trace;

use crate::estimator::{Estimation, Estimator};
use crate::ant::colony::AntColony;
use crate::ant::common::Instance;

pub struct AntColonyEstimator {
    inner: AntColony,
    keepalive: f64,
    round_mul: f64,
}

impl AntColonyEstimator {
    pub fn new(colony: AntColony, keepalive: f64, round_mul: f64) -> Self {
        Self {
            inner: colony,
            keepalive,
            round_mul,
        }
    }
}

impl Estimator for AntColonyEstimator {
    type EstimationType = f64;

    fn estimate(&mut self, config: Config, trace: &dyn Trace) -> Estimation<Self::EstimationType> {
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
        let obj = self.inner.run(&instance);
        Estimation::UpperBound((obj as f64) / self.round_mul)
    }
}
