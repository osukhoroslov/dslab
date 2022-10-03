//! Service-level agreement violation (SLAV) models.

use dyn_clone::{clone_trait_object, DynClone};

/// Trait for implementation of SLAV (Service Level Agreement Violation) metric.
///
/// The metric defines the quantity of service level violation caused by the physical host
/// was overloaded for a while and was not able to serve some customers activities.
///
/// It is possible to implement arbitrary function even with power_consumption usage.
pub trait SLAVModel: DynClone {
    /// Every time host`s CPU load is changed this function is called to update the SLAV metric.
    fn update_model(&mut self, time: f64, cpu_load: f64, power_consumption: f64);

    /// Returns the current SLAV metric value for the host for current time point.
    fn get_accumulated_slav(&self) -> f64;
}

clone_trait_object!(SLAVModel);

/// SLA violation Time per Active Host.
///
/// Returns the ratio between the time host was overloaded and it`s active lifetime.
#[derive(Clone)]
pub struct SLATAHModel {
    prev_time: f64,
    prev_cpu_load: f64,
    total_host_uptime: f64,
    total_overloaded_uptime: f64,
}

impl SLATAHModel {
    pub fn new() -> Self {
        Self {
            prev_time: 0.,
            prev_cpu_load: 0.,
            total_host_uptime: 0.,
            total_overloaded_uptime: 0.,
        }
    }
}

impl SLAVModel for SLATAHModel {
    fn update_model(&mut self, time: f64, cpu_load: f64, _power_consumption: f64) {
        let time_delta = time - self.prev_time;

        if self.prev_cpu_load > 0. {
            self.total_host_uptime += time_delta;
        }
        if self.prev_cpu_load >= 1. {
            self.total_overloaded_uptime += time_delta;
        }

        self.prev_time = time;
        self.prev_cpu_load = cpu_load;
    }

    fn get_accumulated_slav(&self) -> f64 {
        self.total_overloaded_uptime / self.total_host_uptime
    }
}
