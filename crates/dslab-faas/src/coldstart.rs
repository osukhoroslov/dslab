use crate::container::Container;
use crate::function::Application;
use crate::invocation::Invocation;

pub trait ColdStartPolicy {
    /// maximum allowed idle time until container destruction
    fn keepalive_window(&mut self, container: &Container) -> f64;
    /// prewarm = x > 0 => destroy container, deploy new container after x time units since execution
    /// prewarm = 0 => do not destroy container immediately after execution
    fn prewarm_window(&mut self, app: &Application) -> f64;
    /// this function allows tuning policy on finished invocations
    fn update(&mut self, invocation: &Invocation, app: &Application);
}

pub struct FixedTimeColdStartPolicy {
    keepalive_window: f64,
    prewarm_window: f64,
}

impl FixedTimeColdStartPolicy {
    pub fn new(keepalive_window: f64, prewarm_window: f64) -> Self {
        Self {
            keepalive_window,
            prewarm_window,
        }
    }
}

impl ColdStartPolicy for FixedTimeColdStartPolicy {
    fn keepalive_window(&mut self, _container: &Container) -> f64 {
        self.keepalive_window
    }

    fn prewarm_window(&mut self, _app: &Application) -> f64 {
        self.prewarm_window
    }

    fn update(&mut self, _invocation: &Invocation, _app: &Application) {}
}
