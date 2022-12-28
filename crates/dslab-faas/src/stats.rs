use std::collections::HashMap;

use order_stat::kth_by;

use crate::invocation::{Invocation, InvocationRequest};
use crate::resource::ResourceConsumer;

/// This struct allows calculating statistical functions on some data sample.
/// Almost all metrics computed by the simulator are stored using this struct.
#[derive(Clone, Default)]
pub struct SampleMetric {
    data: Vec<f64>,
}

impl SampleMetric {
    /// adds a new element to the sample
    pub fn add(&mut self, x: f64) {
        self.data.push(x);
    }

    /// returns the number of elements in this sample
    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
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

    /// returns biased/unbiased sample variance
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

    pub fn values(&self) -> &[f64] {
        &self.data
    }

    /// extends current metric with zeros to given number of samples
    /// if given number of samples is less than current number of samples, does nothing
    pub fn extend_inplace(&mut self, len: usize) {
        while self.data.len() < len {
            self.data.push(0.);
        }
    }

    /// same as extend_inplace, but makes a copy
    pub fn extend(&self, len: usize) -> Self {
        let mut result = self.clone();
        result.extend_inplace(len);
        result
    }
}

#[derive(Clone, Default)]
pub struct InvocationStats {
    pub invocations: u64,
    pub cold_starts: u64,
    /// this metric counts latency of cold starts only, warm starts are not counted as zero
    pub cold_start_latency: SampleMetric,
    /// measures queueing time of requests stuck in the invoker queue (other requests are not
    /// counted at all)
    pub queueing_time: SampleMetric,
    pub abs_exec_slowdown: SampleMetric,
    pub rel_exec_slowdown: SampleMetric,
    pub abs_total_slowdown: SampleMetric,
    pub rel_total_slowdown: SampleMetric,
}

impl InvocationStats {
    pub fn on_cold_start(&mut self, _request: &InvocationRequest, delay: f64) {
        self.cold_start_latency.add(delay);
        self.cold_starts += 1;
    }

    pub fn on_new_invocation(&mut self, _request: &InvocationRequest) {
        self.invocations += 1;
    }

    pub fn update(&mut self, invocation: &Invocation) {
        let len = invocation.finished.unwrap() - invocation.started;
        let total_len = invocation.finished.unwrap() - invocation.request.time;
        self.abs_exec_slowdown.add(len - invocation.request.duration);
        self.rel_exec_slowdown
            .add((len - invocation.request.duration) / invocation.request.duration);
        self.abs_total_slowdown.add(total_len - invocation.request.duration);
        self.rel_total_slowdown
            .add((total_len - invocation.request.duration) / invocation.request.duration);
    }

    pub fn update_queueing_time(&mut self, request: &InvocationRequest, curr_time: f64) {
        self.queueing_time.add(curr_time - request.time);
    }
}

#[derive(Clone, Default)]
pub struct Stats {
    pub invocation_stats: InvocationStats,
    pub wasted_resource_time: HashMap<usize, SampleMetric>,
}

impl Stats {
    pub fn on_cold_start(&mut self, request: &InvocationRequest, delay: f64) {
        self.invocation_stats.on_cold_start(request, delay);
    }

    pub fn on_new_invocation(&mut self, request: &InvocationRequest) {
        self.invocation_stats.on_new_invocation(request);
    }

    pub fn update_invocation_stats(&mut self, invocation: &Invocation) {
        self.invocation_stats.update(invocation);
    }

    pub fn update_queueing_time(&mut self, request: &InvocationRequest, curr_time: f64) {
        self.invocation_stats.update_queueing_time(request, curr_time);
    }

    pub fn update_wasted_resources(&mut self, time: f64, resource: &ResourceConsumer) {
        for (_, req) in resource.iter() {
            let delta = time * (req.quantity as f64);
            self.wasted_resource_time.entry(req.id).or_default().add(delta);
        }
    }
}

#[derive(Clone, Default)]
pub struct StatsMonitor {
    pub func_stats: HashMap<u64, InvocationStats>,
    pub global_stats: Stats,
}

impl StatsMonitor {
    pub fn on_cold_start(&mut self, request: &InvocationRequest, delay: f64) {
        self.global_stats.on_cold_start(request, delay);
        self.func_stats
            .entry(request.func_id)
            .or_default()
            .on_cold_start(request, delay);
    }

    pub fn on_new_invocation(&mut self, request: &InvocationRequest) {
        self.global_stats.on_new_invocation(request);
        self.func_stats
            .entry(request.func_id)
            .or_default()
            .on_new_invocation(request);
    }

    pub fn update_invocation_stats(&mut self, invocation: &Invocation) {
        self.global_stats.update_invocation_stats(invocation);
        self.func_stats
            .entry(invocation.request.func_id)
            .or_default()
            .update(invocation);
    }

    pub fn update_queueing_time(&mut self, request: &InvocationRequest, curr_time: f64) {
        self.global_stats.update_queueing_time(request, curr_time);
        self.func_stats
            .entry(request.func_id)
            .or_default()
            .update_queueing_time(request, curr_time);
    }

    pub fn update_wasted_resources(&mut self, time: f64, resource: &ResourceConsumer) {
        self.global_stats.update_wasted_resources(time, resource);
    }
}
