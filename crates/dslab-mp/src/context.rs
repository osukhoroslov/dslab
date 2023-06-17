use std::cell::RefCell;
use std::rc::Rc;

use dslab_core::SimulationContext;
use rand::{Rng, SeedableRng};
use rand_pcg::Pcg64;

use crate::message::Message;
use crate::node::{ProcessEvent, TimerBehavior};

pub struct Context {
    proc_name: String,
    time: f64,
    rng: Box<dyn RandomProvider>,
    actions: Vec<ProcessEvent>,
}

trait RandomProvider {
    fn rand(&mut self) -> f64;
}

struct SimulationRng {
    sim_ctx: Rc<RefCell<SimulationContext>>,
}

impl RandomProvider for SimulationRng {
    fn rand(&mut self) -> f64 {
        self.sim_ctx.borrow().rand()
    }
}

impl RandomProvider for Pcg64 {
    fn rand(&mut self) -> f64 {
        self.gen_range(0.0..1.0)
    }
}

impl Context {
    pub fn from_simulation(proc_name: String, sim_ctx: Rc<RefCell<SimulationContext>>, clock_skew: f64) -> Self {
        let time = sim_ctx.borrow().time() + clock_skew;
        Self {
            proc_name,
            time,
            rng: Box::new(SimulationRng { sim_ctx }),
            actions: Vec::new(),
        }
    }

    pub fn basic(proc_name: String, time: f64, clock_skew: f64, random_seed: u64) -> Self {
        Self {
            proc_name,
            time: time + clock_skew,
            rng: Box::new(Pcg64::seed_from_u64(random_seed)),
            actions: Vec::new(),
        }
    }

    pub fn time(&self) -> f64 {
        self.time
    }

    pub fn rand(&mut self) -> f64 {
        self.rng.as_mut().rand()
    }

    pub fn send(&mut self, msg: Message, dest: String) {
        self.actions.push(ProcessEvent::MessageSent {
            msg,
            src: self.proc_name.clone(),
            dest,
        });
    }

    pub fn send_local(&mut self, msg: Message) {
        self.actions.push(ProcessEvent::LocalMessageSent { msg });
    }

    pub fn set_timer(&mut self, name: &str, delay: f64) {
        self.actions.push(ProcessEvent::TimerSet {
            name: name.to_string(),
            delay,
            behavior: TimerBehavior::OverrideExisting,
        });
    }

    pub fn set_timer_once(&mut self, name: &str, delay: f64) {
        self.actions.push(ProcessEvent::TimerSet {
            name: name.to_string(),
            delay,
            behavior: TimerBehavior::SetOnce,
        });
    }

    pub fn cancel_timer(&mut self, name: &str) {
        self.actions
            .push(ProcessEvent::TimerCancelled { name: name.to_string() });
    }

    pub(crate) fn actions(&mut self) -> Vec<ProcessEvent> {
        self.actions.drain(..).collect()
    }
}
