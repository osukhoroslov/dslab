use std::boxed::Box;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use crate::config::parse_options;
use crate::container::Container;
use crate::function::Application;
use crate::invocation::Invocation;

pub trait ColdStartConvertHelper {
    fn box_to_rc(self: Box<Self>) -> Rc<RefCell<dyn ColdStartPolicy>>;
}

impl<T: 'static + ColdStartPolicy> ColdStartConvertHelper for T {
    fn box_to_rc(self: Box<Self>) -> Rc<RefCell<dyn ColdStartPolicy>> {
        Rc::new(RefCell::new(*self))
    }
}

pub trait ColdStartPolicy: ColdStartConvertHelper {
    /// maximum allowed idle time until container destruction
    fn keepalive_window(&mut self, container: &Container) -> f64;
    /// prewarm = x > 0 => destroy container, deploy new container after x time units since execution
    /// prewarm = 0 => do not destroy container immediately after execution
    fn prewarm_window(&mut self, app: &Application) -> f64;
    /// this function allows tuning policy on finished invocations
    fn update(&mut self, invocation: &Invocation, app: &Application);

    fn to_string(&self) -> String {
        "STUB COLDSTART POLICY NAME".to_string()
    }
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

    pub fn from_options_map(options: &HashMap<String, String>) -> Self {
        let keepalive = options.get("keepalive").unwrap().parse::<f64>().unwrap();
        let prewarm = options.get("prewarm").unwrap().parse::<f64>().unwrap();
        Self::new(keepalive, prewarm)
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

    fn to_string(&self) -> String {
        format!(
            "FixedTimeColdStartPolicy[keepalive={:.2},prewarm{:.2}]",
            self.keepalive_window, self.prewarm_window
        )
    }
}

pub fn default_coldstart_policy_resolver(s: &str) -> Box<dyn ColdStartPolicy> {
    if s == "No unloading" {
        return Box::new(FixedTimeColdStartPolicy::new(f64::MAX / 10.0, 0.0));
    }
    if s.len() >= 26 && &s[0..25] == "FixedTimeColdStartPolicy[" && s.ends_with(']') {
        let opts = parse_options(&s[25..s.len() - 1]);
        return Box::new(FixedTimeColdStartPolicy::from_options_map(&opts));
    }
    panic!("Can't resolve: {}", s);
}
