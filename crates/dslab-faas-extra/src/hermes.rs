use std::cell::RefCell;
use std::rc::Rc;

use serverless::function::Application;
use serverless::host::Host;
use serverless::scheduler::Scheduler;

use crate::simple_schedulers::LeastLoadedScheduler;

pub struct HermesScheduler {
    high_load: LeastLoadedScheduler,
}

impl HermesScheduler {
    pub fn new() -> Self {
        Self {
            high_load: LeastLoadedScheduler::new(true),
        }
    }
}

impl Scheduler for HermesScheduler {
    fn select_host(&mut self, app: &Application, hosts: &Vec<Rc<RefCell<Host>>>) -> usize {
        let mut ans = 0;
        // 0 -> empty, no warm container
        // 1 -> empty, warm container
        // 2 -> non-empty, no warm container
        // 3 -> non-empty, warm container
        let mut priority = -1;
        for (i, host) in hosts.iter().enumerate() {
            let h = host.borrow();
            if h.get_active_invocations() < (h.get_cpu_cores() as u64) {
                let curr_priority;
                if h.get_active_invocations() > 0 {
                    if h.can_invoke(app, false) {
                        curr_priority = 3;
                    } else {
                        curr_priority = 2;
                    }
                } else {
                    if h.can_invoke(app, false) {
                        curr_priority = 1;
                    } else {
                        curr_priority = 0;
                    }
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
        self.high_load.select_host(app, hosts)
    }
}
