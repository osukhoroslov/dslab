use std::boxed::Box;
use std::cell::RefCell;
use std::collections::HashMap;
use std::ops::Deref;
use std::rc::Rc;
use std::str::FromStr;

use rand::prelude::*;
use rand_pcg::Pcg64;

use crate::config::parse_options;
use crate::function::Application;
use crate::host::Host;

/// Scheduler chooses an invoker to run new invocation of some function from given application.
pub trait Scheduler {
    fn select_host(&mut self, app: &Application, hosts: &[Rc<RefCell<Host>>]) -> usize;

    fn to_string(&self) -> String {
        "STUB SCHEDULER NAME".to_string()
    }
}

/// BasicScheduler chooses the first invoker that can hotstart the invocation,
/// otherwise it chooses the first invoker that can deploy the container.
pub struct BasicScheduler {}

impl Scheduler for BasicScheduler {
    fn select_host(&mut self, app: &Application, hosts: &[Rc<RefCell<Host>>]) -> usize {
        for (i, host) in hosts.iter().enumerate() {
            if host.borrow().can_invoke(app, true) {
                return i;
            }
        }
        for (i, host) in hosts.iter().enumerate() {
            if host.borrow().can_allocate(app.get_resources()) {
                return i;
            }
        }
        0
    }

    fn to_string(&self) -> String {
        "BasicScheduler".to_string()
    }
}

pub struct ApplicationHasher {
    pub hash_fn: Box<dyn Fn(u64) -> u64>,
    pub name: String,
}

impl FromStr for ApplicationHasher {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case("identity") {
            return Ok(Self::new(Box::new(|a| a), "Identity".to_string()));
        }
        let mut a: u64 = 0;
        let mut b: u64 = 0;
        for (i, x) in s.split('.').enumerate() {
            if i >= 2 {
                return Err("Too many tokens in hasher string.".to_string());
            }
            if i == 0 {
                if let Ok(y) = x.parse::<u64>() {
                    a = y;
                } else {
                    return Err("Couldn't parse parameter 'a'".to_string());
                }
            } else if let Ok(y) = x.parse::<u64>() {
                b = y;
            } else {
                return Err("Couldn't parse parameter 'b'".to_string());
            }
        }
        let name = format!("Linear[{} * x + {}]", a, b);
        Ok(Self::new(Box::new(move |x| a.wrapping_mul(x).wrapping_add(b)), name))
    }
}

impl ApplicationHasher {
    pub fn new(hash_fn: Box<dyn Fn(u64) -> u64>, name: String) -> Self {
        Self { hash_fn, name }
    }

    pub fn hash(&self, app: u64) -> u64 {
        (self.hash_fn)(app)
    }
}

/// LocalityBasedScheduler picks a host based on application hash.
/// In case host number `i` can't invoke, the scheduler considers host number `(i + step) % hosts.len()`.
pub struct LocalityBasedScheduler {
    hasher: ApplicationHasher,
    step: usize,
    warm_only: bool,
}

impl LocalityBasedScheduler {
    pub fn new(hasher: Option<ApplicationHasher>, step: Option<usize>, warm_only: bool) -> Self {
        let f = hasher.unwrap_or_else(|| ApplicationHasher::new(Box::new(|a| a), "Identity".to_string()));
        let s = step.unwrap_or(1);
        Self {
            hasher: f,
            step: s,
            warm_only,
        }
    }

    pub fn from_options_map(options: &HashMap<String, String>) -> Self {
        let hasher =
            ApplicationHasher::from_str(options.get("hasher").map(|a| a.deref()).unwrap_or("Identity")).unwrap();
        let warm_only = options.get("warm_only").unwrap().parse::<bool>().unwrap();
        let step = options.get("step").map(|s| s.parse::<usize>().unwrap());
        Self::new(Some(hasher), step, warm_only)
    }
}

impl Scheduler for LocalityBasedScheduler {
    fn select_host(&mut self, app: &Application, hosts: &[Rc<RefCell<Host>>]) -> usize {
        let start_idx = (self.hasher.hash(app.id) % (hosts.len() as u64)) as usize;
        let mut cycle = false;
        let mut idx = start_idx;
        while !cycle {
            if hosts[idx].borrow().can_invoke(app, false) {
                break;
            }
            if !self.warm_only && hosts[idx].borrow().can_allocate(app.get_resources()) {
                break;
            }
            idx = (idx + self.step) % hosts.len();
            if idx == start_idx {
                cycle = true;
            }
        }
        if cycle {
            start_idx
        } else {
            idx
        }
    }

    fn to_string(&self) -> String {
        format!(
            "LocalityBasedScheduler[hasher=[{}],step={},warm_only={}]",
            &self.hasher.name, self.step, self.warm_only
        )
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

    pub fn from_options_map(options: &HashMap<String, String>) -> Self {
        let seed = options.get("seed").unwrap().parse::<u64>().unwrap();
        Self::new(seed)
    }
}

impl Scheduler for RandomScheduler {
    fn select_host(&mut self, _app: &Application, hosts: &[Rc<RefCell<Host>>]) -> usize {
        self.rng.gen::<usize>() % hosts.len()
    }

    fn to_string(&self) -> String {
        "RandomScheduler".to_string()
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

    pub fn from_options_map(options: &HashMap<String, String>) -> Self {
        let prefer_warm = options.get("prefer_warm").unwrap().parse::<bool>().unwrap();
        Self::new(prefer_warm)
    }
}

impl Scheduler for LeastLoadedScheduler {
    fn select_host(&mut self, app: &Application, hosts: &[Rc<RefCell<Host>>]) -> usize {
        let mut best = 0;
        let mut best_load = f64::MAX;
        let mut warm = false;
        for (i, host) in hosts.iter().enumerate() {
            let load = host.borrow().get_cpu_load();
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

    fn to_string(&self) -> String {
        format!("LeastLoadedScheduler[prefer_warm={}]", self.prefer_warm)
    }
}

/// RoundRobinScheduler chooses hosts in a circular fashion.
#[derive(Default)]
pub struct RoundRobinScheduler {
    index: usize,
}

impl RoundRobinScheduler {
    pub fn new() -> Self {
        Default::default()
    }
}

impl Scheduler for RoundRobinScheduler {
    fn select_host(&mut self, _app: &Application, hosts: &[Rc<RefCell<Host>>]) -> usize {
        self.index %= hosts.len();
        let chosen = self.index;
        self.index += 1;
        chosen
    }

    fn to_string(&self) -> String {
        "RoundRobinScheduler".to_string()
    }
}

pub fn default_scheduler_resolver(s: &str) -> Box<dyn Scheduler> {
    if s == "BasicScheduler" {
        return Box::new(BasicScheduler {});
    }
    if s == "RoundRobinScheduler" {
        return Box::new(RoundRobinScheduler::new());
    }
    if s.len() >= 17 && &s[0..16] == "RandomScheduler[" && s.ends_with(']') {
        let opts = parse_options(&s[16..s.len() - 1]);
        return Box::new(RandomScheduler::from_options_map(&opts));
    }
    if s.len() >= 22 && &s[0..21] == "LeastLoadedScheduler[" && s.ends_with(']') {
        let opts = parse_options(&s[21..s.len() - 1]);
        return Box::new(LeastLoadedScheduler::from_options_map(&opts));
    }
    if s.len() >= 24 && &s[0..23] == "LocalityBasedScheduler[" && s.ends_with(']') {
        let opts = parse_options(&s[23..s.len() - 1]);
        return Box::new(LocalityBasedScheduler::from_options_map(&opts));
    }
    panic!("Can't resolve: {}", s);
}
