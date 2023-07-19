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

pub enum KeepaliveDecision {
    /// A new keepalive window `w` is chosen for the container.
    /// The container will be deallocated after `w` time units.
    NewWindow(f64),
    /// Nothing changes, the container will be deallocated after the old keepalive window passes.
    OldWindow,
    /// The container must be terminated right now.
    TerminateNow,
}

pub trait ColdStartPolicy: ColdStartConvertHelper {
    /// Sets delay before container deallocation.
    fn keepalive_decision(&mut self, container: &Container) -> KeepaliveDecision;
    /// Prewarm = x > 0 => destroy container, deploy new container after x time units since execution.
    /// Prewarm = 0 => do not destroy container immediately after execution.
    fn prewarm_window(&mut self, app: &Application) -> f64;
    /// This function allows tuning policy on finished invocations.
    fn update(&mut self, invocation: &Invocation, app: &Application);

    fn to_string(&self) -> String {
        "STUB COLDSTART POLICY NAME".to_string()
    }
}

pub struct FixedTimeColdStartPolicy {
    keepalive_window: f64,
    prewarm_window: f64,
    /// If true, keepalive time is reset to new `keepalive_window` after each invocation of the policy.
    /// Otherwise keepalive is set only after the first invocation. False by default.
    reset_keepalive: bool,
    already_set: HashMap<(usize, usize), f64>,
}

impl FixedTimeColdStartPolicy {
    pub fn new(keepalive_window: f64, prewarm_window: f64, reset_keepalive: bool) -> Self {
        Self {
            keepalive_window,
            prewarm_window,
            reset_keepalive,
            already_set: Default::default(),
        }
    }

    pub fn from_options_map(options: &HashMap<String, String>) -> Self {
        let keepalive = options.get("keepalive").unwrap().parse::<f64>().unwrap();
        let prewarm = options.get("prewarm").unwrap().parse::<f64>().unwrap();
        let reset = options
            .get("reset_keepalive")
            .map(|x| x.parse::<bool>().unwrap())
            .unwrap_or(false);
        Self::new(keepalive, prewarm, reset)
    }
}

impl ColdStartPolicy for FixedTimeColdStartPolicy {
    fn keepalive_decision(&mut self, container: &Container) -> KeepaliveDecision {
        if !self.reset_keepalive {
            if let Some(t) = self.already_set.get(&(container.host_id, container.id)) {
                if t - container.last_change <= 0.0 {
                    // last_change should be equal to current time
                    return KeepaliveDecision::TerminateNow;
                } else {
                    return KeepaliveDecision::OldWindow;
                }
            }
            self.already_set.insert(
                (container.host_id, container.id),
                container.last_change + self.keepalive_window,
            );
        }
        KeepaliveDecision::NewWindow(self.keepalive_window)
    }

    fn prewarm_window(&mut self, _app: &Application) -> f64 {
        self.prewarm_window
    }

    fn update(&mut self, _invocation: &Invocation, _app: &Application) {}

    fn to_string(&self) -> String {
        format!(
            "FixedTimeColdStartPolicy[keepalive={:.2},prewarm={:.2},reset_keepalive={}]",
            self.keepalive_window, self.prewarm_window, self.reset_keepalive
        )
    }
}

pub fn default_coldstart_policy_resolver(s: &str) -> Box<dyn ColdStartPolicy> {
    if s == "No unloading" {
        return Box::new(FixedTimeColdStartPolicy::new(f64::MAX / 10.0, 0.0, true));
    }
    if s.len() >= 26 && &s[0..25] == "FixedTimeColdStartPolicy[" && s.ends_with(']') {
        let opts = parse_options(&s[25..s.len() - 1]);
        return Box::new(FixedTimeColdStartPolicy::from_options_map(&opts));
    }
    panic!("Can't resolve: {}", s);
}
