use std::collections::HashMap;

use order_stat::kth_by;

use crate::invocation::Invocation;
use crate::resource::ResourceConsumer;

#[derive(Clone, Default)]
pub struct SampleMetric {
    data: Vec<f64>,
}

impl SampleMetric {
    pub fn add(&mut self, x: f64) {
        self.data.push(x);
    }

    pub fn sum(&self) -> f64 {
        let mut s = 0.0;
        for x in self.data.iter().copied() {
            s += x;
        }
        s
    }

    pub fn mean(&self) -> f64 {
        self.sum() / (self.data.len() as f64)
    }

    pub fn min(&self) -> Option<f64> {
        self.data.iter().copied().reduce(f64::min)
    }

    pub fn max(&self) -> Option<f64> {
        self.data.iter().copied().reduce(f64::max)
    }

    pub fn ordered_statistic(&self, idx: usize) -> f64 {
        debug_assert!(1 <= idx && idx <= self.data.len());
        let mut tmp = self.data.clone();
        *kth_by(&mut tmp, idx - 1, |x, y| x.total_cmp(y))
    }

    /// q-th sample quantile, 0 <= q <= 1
    /// estimation method corresponds to R-7 (the default method in R.stats)
    pub fn quantile(&self, q: f64) -> f64 {
        debug_assert!((0. ..=1.).contains(&q));
        debug_assert!(!self.data.is_empty());
        let h = ((self.data.len() - 1) as f64) * q + 1.;
        let fl = h.floor();
        let k1 = (fl + 1e-9) as usize;
        let k2 = (h.ceil() + 1e-9) as usize;
        let s1 = self.ordered_statistic(k1);
        s1 + (h - fl) * (self.ordered_statistic(k2) - s1)
    }

    pub fn variance(&self, biased: bool) -> f64 {
        let mean = self.mean();
        let mut var = 0.;
        for x in self.data.iter().copied() {
            var += (x - mean) * (x - mean);
        }
        if biased {
            var / (self.data.len() as f64)
        } else if self.data.len() == 1 {
            0.0
        } else {
            var / ((self.data.len() as f64) - 1.0)
        }
    }

    pub fn biased_variance(&self) -> f64 {
        self.variance(true)
    }

    pub fn unbiased_variance(&self) -> f64 {
        self.variance(false)
    }
}

#[derive(Clone, Default)]
pub struct Stats {
    pub invocations: u64,
    pub cold_starts: u64,
    pub cold_start_latency: SampleMetric,
    pub wasted_resource_time: HashMap<usize, SampleMetric>,
    pub abs_exec_slowdown: SampleMetric,
    pub rel_exec_slowdown: SampleMetric,
    pub abs_total_slowdown: SampleMetric,
    pub rel_total_slowdown: SampleMetric,
}

impl Stats {
    pub fn update_invocation_stats(&mut self, invocation: &Invocation) {
        let len = invocation.finished.unwrap() - invocation.started;
        let total_len = invocation.finished.unwrap() - invocation.request.time;
        self.abs_exec_slowdown.add(len - invocation.request.duration);
        self.rel_exec_slowdown
            .add((len - invocation.request.duration) / invocation.request.duration);
        self.abs_total_slowdown.add(total_len - invocation.request.duration);
        self.rel_total_slowdown
            .add((total_len - invocation.request.duration) / invocation.request.duration);
    }

    pub fn update_wasted_resources(&mut self, time: f64, resource: &ResourceConsumer) {
        for (_, req) in resource.iter() {
            let delta = time * (req.quantity as f64);
            self.wasted_resource_time.entry(req.id).or_default().add(delta);
        }
    }
}
