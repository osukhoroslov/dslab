//! Workload trace trait.
use std::cmp::Ordering;

/// Raw application data.
#[derive(Default, Clone)]
pub struct ApplicationData {
    /// Maximum number of invocations that can run simultaneously on one container of this application.
    pub concurrent_invocations: usize,
    /// Time needed to deploy one container of this application.
    pub container_deployment_time: f64,
    /// CPU share required by containers of this application.
    pub container_cpu_share: f64,
    /// Host resources required by containers of this application.
    pub container_resources: Vec<(String, u64)>,
}

impl ApplicationData {
    /// Creates new ApplicationData.
    pub fn new(
        concurrent_invocations: usize,
        container_deployment_time: f64,
        container_cpu_share: f64,
        container_resources: Vec<(String, u64)>,
    ) -> Self {
        Self {
            concurrent_invocations,
            container_deployment_time,
            container_cpu_share,
            container_resources,
        }
    }
}

/// Raw invocation request data.
#[derive(Default, Clone, Copy)]
pub struct RequestData {
    /// Function id.
    pub id: usize,
    /// Invocation duration.
    pub duration: f64,
    /// Request arrival time.
    pub time: f64,
}

impl PartialEq for RequestData {
    fn eq(&self, other: &Self) -> bool {
        self.time == other.time
    }
}

impl Eq for RequestData {}

impl PartialOrd for RequestData {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RequestData {
    fn cmp(&self, other: &Self) -> Ordering {
        self.time.total_cmp(&other.time)
    }
}

/// Workload trace.
pub trait Trace {
    /// Returns an iterator over the applications.
    fn app_iter(&self) -> Box<dyn Iterator<Item = ApplicationData> + '_>;
    /// Returns an iterator over the invocation requests.
    fn request_iter(&self) -> Box<dyn Iterator<Item = RequestData> + '_>;
    /// Returns an iterator over the functions.
    fn function_iter(&self) -> Box<dyn Iterator<Item = usize> + '_>;
    /// Indicates whether the requests produced by `request_iter` are ordered in non-decreasing order of their time.
    fn is_ordered_by_time(&self) -> bool;
    /// Optionally returns simulation end time.
    fn simulation_end(&self) -> Option<f64>;
}
