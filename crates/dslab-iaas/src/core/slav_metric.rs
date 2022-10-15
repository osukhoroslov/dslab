//! Service-level agreement violation metrics.

use dyn_clone::{clone_trait_object, DynClone};

/// Trait for implementation of host-level SLA violation metric.
///
/// This metric measures the amount of SLA violation caused by the host overload,
/// when it is not able to provide the full performance to hosted VMs.
pub trait HostSLAVMetric: DynClone {
    /// Called whenever the host's CPU load is changed to update the metric value.
    fn update(&mut self, time: f64, cpu_load: f64);

    /// Returns the current metric value.
    fn value(&self) -> f64;
}

clone_trait_object!(HostSLAVMetric);

/// Overload Time Fraction (OTF) metric.
///
/// `OTF = T_overload / T_active`
/// - `T_overload` is the total time during which the host was overloaded (leading to an SLA violation).
/// - `T_active` is the total time the host was active (running VMs).
#[derive(Clone)]
pub struct OverloadTimeFraction {
    prev_time: f64,
    prev_cpu_load: f64,
    total_active_time: f64,
    total_overloaded_time: f64,
}

impl OverloadTimeFraction {
    pub fn new() -> Self {
        Self {
            prev_time: 0.,
            prev_cpu_load: 0.,
            total_active_time: 0.,
            total_overloaded_time: 0.,
        }
    }
}

impl HostSLAVMetric for OverloadTimeFraction {
    fn update(&mut self, time: f64, cpu_load: f64) {
        let time_delta = time - self.prev_time;

        if self.prev_cpu_load > 0. {
            self.total_active_time += time_delta;
        }
        if self.prev_cpu_load >= 1. {
            self.total_overloaded_time += time_delta;
        }

        self.prev_time = time;
        self.prev_cpu_load = cpu_load;
    }

    fn value(&self) -> f64 {
        self.total_overloaded_time / self.total_active_time
    }
}
