#[derive(Default, Clone)]
pub struct ApplicationData {
    pub concurrent_invocations: usize,
    pub container_deployment_time: f64,
    pub container_cpu_share: f64,
    pub container_resources: Vec<(String, u64)>,
}

impl ApplicationData {
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

#[derive(Default, Clone, Copy)]
pub struct RequestData {
    pub id: u64,
    pub duration: f64,
    pub time: f64,
}

pub trait Trace {
    fn app_iter(&self) -> Box<dyn Iterator<Item = ApplicationData> + '_>;
    fn request_iter(&self) -> Box<dyn Iterator<Item = RequestData> + '_>;
    fn function_iter(&self) -> Box<dyn Iterator<Item = u64> + '_>;
    fn simulation_end(&self) -> Option<f64>;
}
