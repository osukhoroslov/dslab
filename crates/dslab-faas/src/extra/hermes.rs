use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use crate::function::Application;
use crate::host::Host;
use crate::scheduler::LeastLoadedScheduler;
use crate::scheduler::Scheduler;

/// Refer to https://arxiv.org/abs/2111.07226
pub struct HermesScheduler {
    high_load_fallback: LeastLoadedScheduler,
    use_invocation_count: bool,
    avoid_queueing: bool,
}

impl HermesScheduler {
    pub fn new(use_invocation_count: bool, avoid_queueing: bool) -> Self {
        Self {
            high_load_fallback: LeastLoadedScheduler::new(true, use_invocation_count, avoid_queueing),
            use_invocation_count,
            avoid_queueing,
        }
    }

    pub fn from_options_map(options: &HashMap<String, String>) -> Self {
        let use_invocation_count = options.get("use_invocation_count").unwrap().parse::<bool>().unwrap();
        let avoid_queueing = options.get("avoid_queueing").unwrap().parse::<bool>().unwrap();
        Self::new(use_invocation_count, avoid_queueing)
    }
}

impl Scheduler for HermesScheduler {
    fn select_host(&mut self, app: &Application, hosts: &[Rc<RefCell<Host>>]) -> usize {
        let mut ans = 0;
        // 0 -> empty, no warm container
        // 1 -> empty, warm container
        // 2 -> non-empty, no warm container
        // 3 -> non-empty, warm container
        let mut priority = -1;
        for (i, host) in hosts.iter().enumerate() {
            let h = host.borrow();
            let val = if self.use_invocation_count {
                (h.get_all_invocations() as f64) + 1e-9
            } else {
                h.get_cpu_load() + 1e-9
            };
            if val < (h.get_cpu_cores() as f64) {
                let mut curr_priority = -1;
                if h.get_all_invocations() > 0 {
                    if h.can_invoke(app, false) {
                        curr_priority = 3;
                    } else if !self.avoid_queueing || h.can_allocate(app.get_resources()) {
                        curr_priority = 2;
                    }
                } else if h.can_invoke(app, false) {
                    curr_priority = 1;
                } else if !self.avoid_queueing || h.can_allocate(app.get_resources()) {
                    curr_priority = 0;
                }
                if curr_priority > priority {
                    priority = curr_priority;
                    ans = i;
                }
            }
        }
        if priority != -1 {
            return ans;
        }
        self.high_load_fallback.select_host(app, hosts)
    }

    fn to_string(&self) -> String {
        format!(
            "HermesScheduler[use_invocation_count={},avoid_queueing={}]",
            self.use_invocation_count, self.avoid_queueing
        )
    }
}
