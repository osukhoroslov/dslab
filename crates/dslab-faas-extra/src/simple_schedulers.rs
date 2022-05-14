use std::cell::RefCell;
use std::rc::Rc;

use rand::prelude::*;
use rand_pcg::Pcg64;

use serverless::function::Application;
use serverless::host::Host;
use serverless::scheduler::Scheduler;

pub struct LocalityBasedScheduler {
    hasher: fn(u64) -> u64,
    step: usize,
}

impl LocalityBasedScheduler {
    pub fn new(hasher: Option<fn(u64) -> u64>, step: Option<usize>) -> Self {
        let f = hasher.unwrap_or(|a| a);
        let s = step.unwrap_or(1);
        Self { hasher: f, step: s }
    }
}

impl Scheduler for LocalityBasedScheduler {
    fn select_host(&mut self, app: &Application, hosts: &Vec<Rc<RefCell<Host>>>) -> usize {
        let init = ((self.hasher)(app.id) % (hosts.len() as u64)) as usize;
        let mut cycle = false;
        let mut idx = init;
        while !cycle {
            if hosts[idx].borrow().can_invoke(app, false) {
                break;
            }
            idx = (idx + self.step) % hosts.len();
            if idx == init {
                cycle = true;
            }
        }
        if cycle {
            init
        } else {
            idx
        }
    }
}

pub struct RandomScheduler {
    rng: Pcg64,
}

impl RandomScheduler {
    pub fn new(seed: u64) -> Self {
        Self {
            rng: Pcg64::seed_from_u64(seed),
        }
    }
}

impl Scheduler for RandomScheduler {
    fn select_host(&mut self, _app: &Application, hosts: &Vec<Rc<RefCell<Host>>>) -> usize {
        self.rng.gen::<usize>() % hosts.len()
    }
}

pub struct LeastLoadedScheduler {}

impl Scheduler for LeastLoadedScheduler {
    fn select_host(&mut self, _app: &Application, hosts: &Vec<Rc<RefCell<Host>>>) -> usize {
        let mut best = 0;
        let mut best_load = u64::MAX;
        for (i, host) in hosts.iter().enumerate() {
            let load = host.borrow().get_active_invocations();
            if load < best_load {
                best_load = load;
                best = i;
            }
        }
        best
    }
}

pub struct RoundRobinScheduler {
    index: usize,
}

impl RoundRobinScheduler {
    pub fn new() -> Self {
        Self { index: 0 }
    }
}

impl Scheduler for RoundRobinScheduler {
    fn select_host(&mut self, _app: &Application, hosts: &Vec<Rc<RefCell<Host>>>) -> usize {
        self.index %= hosts.len();
        let chosen = self.index;
        self.index += 1;
        chosen
    }
}
