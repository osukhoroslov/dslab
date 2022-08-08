use std::cell::RefCell;
use std::rc::Rc;

use rand::prelude::*;
use rand_pcg::Pcg64;

use dslab_faas::function::Application;
use dslab_faas::host::Host;
use dslab_faas::scheduler::Scheduler;

/// LocalityBasedScheduler picks a host based on appication hash.
/// In case host number i can't invoke, the scheduler considers host number (i + step)%hosts.len().
pub struct LocalityBasedScheduler {
    hasher: fn(u64) -> u64,
    step: usize,
    warm_only: bool,
}

impl LocalityBasedScheduler {
    pub fn new(hasher: Option<fn(u64) -> u64>, step: Option<usize>, warm_only: bool) -> Self {
        let f = hasher.unwrap_or(|a| a);
        let s = step.unwrap_or(1);
        Self {
            hasher: f,
            step: s,
            warm_only,
        }
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
            if !self.warm_only && hosts[idx].borrow().can_allocate(app.get_resources()) {
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

/// RandomScheduler picks a host uniformly at random.
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

/// LeastLoadedScheduler chooses a host with the least number of active (running and queued) invocations.
pub struct LeastLoadedScheduler {
    /// break ties by preferring instances with warm containers
    prefer_warm: bool,
}

impl LeastLoadedScheduler {
    pub fn new(prefer_warm: bool) -> Self {
        Self { prefer_warm }
    }
}

impl Scheduler for LeastLoadedScheduler {
    fn select_host(&mut self, app: &Application, hosts: &Vec<Rc<RefCell<Host>>>) -> usize {
        let mut best = 0;
        let mut best_load = u64::MAX;
        let mut warm = false;
        for (i, host) in hosts.iter().enumerate() {
            let load = host.borrow().get_active_invocations();
            if load < best_load {
                best_load = load;
                best = i;
                if self.prefer_warm {
                    warm = host.borrow().can_invoke(app, false);
                }
            } else if load == best_load && self.prefer_warm && !warm && host.borrow().can_invoke(app, false) {
                best = i;
                warm = true;
            }
        }
        best
    }
}

/// RoundRobinScheduler chooses hosts in a circular fashion.
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
