/// This file contains common base for all local search methods.
/// Including optimization goal and problem instance.

pub const EPS: f64 = 1e-9;

#[derive(PartialEq, Eq)]
pub enum OptimizationGoal {
    Minimization,
    Maximization,
}

impl OptimizationGoal {
    pub fn is_better(&self, l: f64, r: f64) -> bool {
        if *self == OptimizationGoal::Minimization {
            l < r
        } else {
            r < l
        }
    }
}

#[derive(Clone, Default)]
pub struct Instance {
    pub hosts: Vec<Vec<u64>>,
    pub apps: Vec<Vec<u64>>,
    pub app_coldstart: Vec<f64>,
    pub req_app: Vec<usize>,
    pub req_dur: Vec<f64>,
    pub req_start: Vec<f64>,
    pub keepalive: f64,
}

#[derive(Clone, Default)]
pub struct Container {
    pub host: usize,
    pub app: usize,
    pub invocations: Vec<usize>,
    pub resources: Vec<u64>,
    pub start: f64,
    pub end: f64,
}

#[derive(Clone, Default)]
pub struct State {
    pub containers: Vec<Container>,
    pub objective: f64,
}

impl State {
    pub fn validate(&self, instance: &Instance) -> Result<(), String> {
        let w = instance.keepalive;
        for (c_id, c) in self.containers.iter().enumerate() {
            let mut start = c.start + instance.app_coldstart[instance.req_app[c.invocations[0]]];
            for id in c.invocations.iter().copied() {
                if start + w < instance.req_start[id] - EPS {
                    return Err(format!(
                        "Keepalive time exceeded within container {} (stalling invocation = {})",
                        c_id, id
                    ));
                }
                start = start.max(instance.req_start[id]) + instance.req_dur[id];
            }
            if (start + w - c.end).abs() > EPS {
                return Err(format!(
                    "Wrong end time set for container {}: found {}, but must be {}",
                    c_id,
                    c.end,
                    start + w
                ));
            }
        }
        for r in 0..instance.hosts[0].len() {
            let mut events = vec![Vec::<(f64, i64)>::new(); instance.hosts.len()];
            for c in self.containers.iter() {
                events[c.host].push((c.start, c.resources[r] as i64));
                events[c.host].push((c.end, -(c.resources[r] as i64)));
            }
            for h in 0..events.len() {
                events[h].sort_by(|a, b| a.0.total_cmp(&b.0));
                let mut ptr = 0;
                let mut sum = 0i64;
                while ptr < events[h].len() {
                    let mut ptr2 = ptr;
                    while ptr2 < events[h].len() && (events[h][ptr2].0 - events[h][ptr].0).abs() < EPS {
                        sum += events[h][ptr2].1;
                        ptr2 += 1;
                    }
                    if TryInto::<u64>::try_into(sum).unwrap() > instance.hosts[h][r] {
                        return Err(format!(
                            "Resource {} exceeded on host {} at time {}",
                            r, h, events[h][ptr].0
                        ));
                    }
                    ptr = ptr2;
                }
            }
        }
        Ok(())
    }

    pub fn recompute_objective(&mut self, instance: &Instance) {
        self.objective = 0.;
        for c in self.containers.iter() {
            let f = instance.req_app[c.invocations[0]];
            self.objective += instance.app_coldstart[f];
        }
    }
}
