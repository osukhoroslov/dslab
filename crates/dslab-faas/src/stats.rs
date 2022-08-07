use std::collections::HashMap;
use std::ops::AddAssign;

use num::{ToPrimitive, Zero};

use crate::invocation::Invocation;
use crate::resource::ResourceConsumer;

#[derive(Clone, Default)]
pub struct SampleMetric<T> {
    data: Vec<T>,
}

impl<T> SampleMetric<T> {
    pub fn add(&mut self, x: T) {
        self.data.push(x);
    }
}

impl<T> SampleMetric<T>
where
    T: AddAssign + Copy + Zero,
{
    pub fn sum(&self) -> T {
        let mut s = T::zero();
        for x in self.data.iter().copied() {
            s += x;
        }
        s
    }
}

impl<T> SampleMetric<T>
where
    T: AddAssign + Copy + Zero + ToPrimitive,
{
    pub fn mean(&self) -> f64 {
        self.sum().to_f64().unwrap() / (self.data.len() as f64)
    }
}

#[derive(Clone, Default)]
pub struct Stats {
    pub invocations: u64,
    pub cold_starts: u64,
    pub cold_start_latency: SampleMetric<f64>,
    pub wasted_resource_time: HashMap<usize, SampleMetric<f64>>,
    pub abs_slowdown: SampleMetric<f64>,
    pub rel_slowdown: SampleMetric<f64>,
}

impl Stats {
    pub fn update_invocation_stats(&mut self, invocation: &Invocation) {
        let len = invocation.finished.unwrap() - invocation.started;
        self.abs_slowdown.add(len - invocation.request.duration);
        self.rel_slowdown
            .add((len - invocation.request.duration) / invocation.request.duration);
    }

    pub fn update_wasted_resources(&mut self, time: f64, resource: &ResourceConsumer) {
        for (_, req) in resource.iter() {
            let delta = time * (req.quantity as f64);
            self.wasted_resource_time.entry(req.id).or_default().add(delta);
        }
    }
}
