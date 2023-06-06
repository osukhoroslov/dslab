use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use crate::function::Application;
use crate::host::Host;
use crate::scheduler::LeastLoadedScheduler;
use crate::scheduler::Scheduler;

/// Refer to <https://dl.acm.org/doi/abs/10.1145/3542929.3563468>
pub struct HermodScheduler {
    high_load_fallback: LeastLoadedScheduler,
    prefer_warm: bool,
    use_invocation_count: bool,
    avoid_queueing: bool,
}

impl HermodScheduler {
    pub fn new(prefer_warm: bool, use_invocation_count: bool, avoid_queueing: bool) -> Self {
        Self {
            high_load_fallback: LeastLoadedScheduler::new(prefer_warm, use_invocation_count, avoid_queueing),
            prefer_warm,
            use_invocation_count,
            avoid_queueing,
        }
    }

    pub fn from_options_map(options: &HashMap<String, String>) -> Self {
        let prefer_warm = options.get("prefer_warm").unwrap().parse::<bool>().unwrap();
        let use_invocation_count = options.get("use_invocation_count").unwrap().parse::<bool>().unwrap();
        let avoid_queueing = options.get("avoid_queueing").unwrap().parse::<bool>().unwrap();
        Self::new(prefer_warm, use_invocation_count, avoid_queueing)
    }
}

impl Scheduler for HermodScheduler {
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
                (h.total_invocation_count() as f64) + 1e-9
            } else {
                h.get_cpu_load() + 1e-9
            };
            if val < (h.get_cpu_cores() as f64) {
                let mut curr_priority = -1;
                if h.total_invocation_count() > 0 {
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
            "HermodScheduler[prefer_warm={},use_invocation_count={},avoid_queueing={}]",
            self.prefer_warm, self.use_invocation_count, self.avoid_queueing
        )
    }
}
