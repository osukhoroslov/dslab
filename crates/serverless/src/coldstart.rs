use crate::container::Container;
use crate::function::Function;

pub trait ColdStartPolicy {
    // maximum allowed idle time until container destruction
    fn keepalive_window(&mut self, container: &Container) -> f64;
    // prewarm = None => do not prewarm function
    // prewarm = Some(x > 0) => deploy new container after x time units since execution
    // prewarm = Some(0) => do not destroy container after execution
    fn prewarm_window(&mut self, function: &Function) -> Option<f64>;
}

pub struct FixedTimeColdStartPolicy {
    keepalive: f64,
    prewarm: Option<f64>,
}

impl FixedTimeColdStartPolicy {
    pub fn new(keepalive: f64, prewarm: Option<f64>) -> Self {
        Self { keepalive, prewarm }
    }
}

impl ColdStartPolicy for FixedTimeColdStartPolicy {
    fn keepalive_window(&mut self, container: &Container) -> f64 {
        self.keepalive
    }

    fn prewarm_window(&mut self, function: &Function) -> Option<f64> {
        self.prewarm
    }
}
